#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * CONCURRENT HELPER FUNCTIONS
 *****************************************************************************/

/*
 * Check if the node is safe for the given operation
 * For INSERT: safe means node is not full (size < max_size)
 * For DELETE: safe means node has more than minimum keys
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::IsSafe(N *node, Operation op) -> bool {
  if (op == Operation::SEARCH) {
    return true;
  }
  if (op == Operation::INSERT) {
    // A node is safe if inserting won't cause a split
    // Split happens when size >= max_size (for leaf) or size >= max_size (for internal)
    // So safe means size < max_size - 1 (after insert, size will be < max_size)
    if (node->IsLeafPage()) {
      // Leaf splits when size >= leaf_max_size after insert
      return node->GetSize() < leaf_max_size_ - 1;
    }
    // Internal splits when size >= internal_max_size after insert
    return node->GetSize() < internal_max_size_ - 1;
  }
  // For DELETE
  if (node->IsRootPage()) {
    if (node->IsLeafPage()) {
      return node->GetSize() > 1;
    }
    return node->GetSize() > 2;
  }
  return node->GetSize() > node->GetMinSize();
}

/*
 * Unlock and unpin all pages stored in transaction's page set
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  auto page_set = transaction->GetPageSet();
  for (auto *page : *page_set) {
    if (page == nullptr) {
      // nullptr marks that we hold root latch
      root_latch_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
  page_set->clear();

  // Delete pages marked for deletion
  auto deleted_page_set = transaction->GetDeletedPageSet();
  for (auto page_id : *deleted_page_set) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  deleted_page_set->clear();
}

/*
 * Only unlock pages (don't unpin) - used for search operations
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  auto page_set = transaction->GetPageSet();
  for (auto *page : *page_set) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  page_set->clear();
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Helper function to find leaf page that may contain the key
 * Uses latch crabbing for concurrent access
 * @return the Page* containing the leaf page that may contain the key
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, Operation op, Transaction *transaction) -> Page * {
  // For search, acquire read lock on root
  // For insert/delete, acquire write lock on root
  if (op == Operation::SEARCH) {
    root_latch_.RLock();
  } else {
    root_latch_.WLock();
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(nullptr);  // Mark that we hold root latch
    }
  }

  if (IsEmpty()) {
    if (op == Operation::SEARCH) {
      root_latch_.RUnlock();
    } else {
      if (transaction != nullptr) {
        transaction->GetPageSet()->pop_back();  // Remove the nullptr marker
      }
      root_latch_.WUnlock();
    }
    return nullptr;
  }

  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    if (op == Operation::SEARCH) {
      root_latch_.RUnlock();
    } else {
      if (transaction != nullptr) {
        transaction->GetPageSet()->pop_back();
      }
      root_latch_.WUnlock();
    }
    return nullptr;
  }
  
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (op == Operation::SEARCH) {
    page->RLatch();
    root_latch_.RUnlock();
  } else {
    page->WLatch();
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(page);
    }
  }

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id;
    if (leftMost) {
      child_page_id = internal->ValueAt(0);
    } else {
      child_page_id = internal->Lookup(key, comparator_);
    }

    auto *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    if (child_page == nullptr) {
      // Failed to fetch child page, release all locks and return
      if (op == Operation::SEARCH) {
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      } else {
        if (transaction != nullptr) {
          UnlockUnpinPages(transaction);
        }
      }
      return nullptr;
    }
    
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    if (op == Operation::SEARCH) {
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      child_page->WLatch();
      // For INSERT/DELETE: always keep all ancestors locked (simpler strategy)
      // This is less concurrent but more correct
      if (transaction != nullptr) {
        transaction->AddIntoPageSet(child_page);
      }
    }

    node = child_node;
    page = child_page;
  }

  // For write operations, remove the leaf page from page_set since we return it separately
  if (op != Operation::SEARCH && transaction != nullptr) {
    transaction->GetPageSet()->pop_back();
  }

  return page;
}

/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  auto *page = FindLeafPage(key, false, Operation::SEARCH, transaction);
  if (page == nullptr) {
    return false;
  }

  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  ValueType value;
  bool found = leaf_page->Lookup(key, &value, comparator_);

  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  if (found) {
    result->push_back(value);
  }
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_latch_.WLock();
  if (IsEmpty()) {
    StartNewTree(key, value);
    root_latch_.WUnlock();
    return true;
  }
  root_latch_.WUnlock();
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Create a new tree when inserting the first key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_page_id;
  auto *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page for B+ tree root");
  }

  auto *root = reinterpret_cast<LeafPage *>(page->GetData());
  root->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  root->Insert(key, value, comparator_);

  root_page_id_ = new_page_id;
  UpdateRootPageId(1);

  buffer_pool_manager_->UnpinPage(new_page_id, true);
}

/*
 * Insert into leaf page
 * @return false if duplicate key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  auto *page = FindLeafPage(key, false, Operation::INSERT, transaction);
  if (page == nullptr) {
    // Tree became empty between Insert's check and FindLeafPage
    // This shouldn't happen with proper locking, but handle it gracefully
    return false;
  }

  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  // Check for duplicate key
  ValueType existing_value;
  if (leaf_page->Lookup(key, &existing_value, comparator_)) {
    // Release all locks
    if (transaction != nullptr) {
      UnlockUnpinPages(transaction);
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }

  // Insert into leaf page
  int new_size = leaf_page->Insert(key, value, comparator_);

  // If leaf is full after insert, split
  if (new_size >= leaf_max_size_) {
    auto *new_leaf = Split(leaf_page);
    KeyType new_key = new_leaf->KeyAt(0);
    InsertIntoParent(leaf_page, new_key, new_leaf, transaction);
    buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  }

  // Release all ancestor locks
  if (transaction != nullptr) {
    UnlockUnpinPages(transaction);
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

/*
 * Split the leaf page and return the new page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(LeafPage *leaf_page) -> LeafPage * {
  page_id_t new_page_id;
  auto *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new leaf page for split");
  }

  auto *new_leaf = reinterpret_cast<LeafPage *>(page->GetData());
  new_leaf->Init(new_page_id, leaf_page->GetParentPageId(), leaf_max_size_);

  leaf_page->MoveHalfTo(new_leaf);

  return new_leaf;
}

/*
 * Split the internal page and return the new page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(InternalPage *internal_page) -> InternalPage * {
  page_id_t new_page_id;
  auto *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new internal page for split");
  }

  auto *new_internal = reinterpret_cast<InternalPage *>(page->GetData());
  new_internal->Init(new_page_id, internal_page->GetParentPageId(), internal_max_size_);

  internal_page->MoveHalfTo(new_internal, buffer_pool_manager_);

  return new_internal;
}

/*
 * Insert a new key into parent of node
 * Note: All ancestor pages are already locked and in page_set
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // If old_node is root, create a new root
  if (old_node->IsRootPage()) {
    page_id_t new_root_id;
    auto *page = buffer_pool_manager_->NewPage(&new_root_id);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new root page");
    }

    auto *new_root = reinterpret_cast<InternalPage *>(page->GetData());
    new_root->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);

    root_page_id_ = new_root_id;
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(new_root_id, true);
    return;
  }

  // Find parent page - it's already locked in page_set
  // We need to fetch it to get access (this increases pin count, so we must unpin)
  page_id_t parent_id = old_node->GetParentPageId();
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  new_node->SetParentPageId(parent_id);
  int new_size = parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  // If parent is full, split it
  if (new_size >= internal_max_size_) {
    auto *new_parent = Split(parent);
    KeyType new_key = new_parent->KeyAt(0);
    InsertIntoParent(parent, new_key, new_parent, transaction);
    buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
  }

  // Unpin the extra fetch we did (the original pin from FindLeafPage is in page_set)
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  auto *page = FindLeafPage(key, false, Operation::DELETE, transaction);
  if (page == nullptr) {
    return;
  }

  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  int old_size = leaf_page->GetSize();
  int new_size = leaf_page->RemoveAndDeleteRecord(key, comparator_);

  // Key was not found
  if (new_size == old_size) {
    if (transaction != nullptr) {
      UnlockUnpinPages(transaction);
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return;
  }

  // Check if we need to coalesce or redistribute
  bool should_delete = CoalesceOrRedistribute(leaf_page, transaction);

  if (transaction != nullptr) {
    UnlockUnpinPages(transaction);
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  if (should_delete) {
    buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
  }
}

/*
 * Handle coalesce or redistribute after deletion
 * @return true if node should be deleted
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
  // If node is root
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  // If node has enough keys, no need to coalesce or redistribute
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }

  // Get parent and sibling
  page_id_t parent_id = node->GetParentPageId();
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  int index = parent->ValueIndex(node->GetPageId());

  // Try to borrow from left sibling
  if (index > 0) {
    page_id_t left_sibling_id = parent->ValueAt(index - 1);
    auto *left_sibling_page = buffer_pool_manager_->FetchPage(left_sibling_id);
    auto *left_sibling = reinterpret_cast<N *>(left_sibling_page->GetData());

    // Redistribute from left sibling
    if (left_sibling->GetSize() > left_sibling->GetMinSize()) {
      Redistribute(left_sibling, node, parent, index, true);
      buffer_pool_manager_->UnpinPage(left_sibling_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return false;
    }

    // Coalesce with left sibling
    bool parent_should_delete = Coalesce(left_sibling, node, parent, index, transaction);
    buffer_pool_manager_->UnpinPage(left_sibling_id, true);
    buffer_pool_manager_->UnpinPage(parent_id, true);

    if (parent_should_delete) {
      buffer_pool_manager_->DeletePage(parent_id);
    }
    return true;  // node should be deleted
  }

  // Try to borrow from right sibling
  if (index < parent->GetSize() - 1) {
    page_id_t right_sibling_id = parent->ValueAt(index + 1);
    auto *right_sibling_page = buffer_pool_manager_->FetchPage(right_sibling_id);
    auto *right_sibling = reinterpret_cast<N *>(right_sibling_page->GetData());

    // Redistribute from right sibling
    if (right_sibling->GetSize() > right_sibling->GetMinSize()) {
      Redistribute(right_sibling, node, parent, index, false);
      buffer_pool_manager_->UnpinPage(right_sibling_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return false;
    }

    // Coalesce with right sibling (move right sibling into node)
    bool parent_should_delete = Coalesce(node, right_sibling, parent, index + 1, transaction);

    buffer_pool_manager_->UnpinPage(parent_id, true);

    if (parent_should_delete) {
      buffer_pool_manager_->DeletePage(parent_id);
    }

    // Delete right sibling
    buffer_pool_manager_->UnpinPage(right_sibling_id, true);
    buffer_pool_manager_->DeletePage(right_sibling_id);

    return false;  // node should not be deleted
  }

  buffer_pool_manager_->UnpinPage(parent_id, false);
  return false;
}

/*
 * Handle root adjustment after deletion
 * @return true if old root should be deleted
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool {
  // Case 1: root is leaf with no elements
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }

  // Case 2: root is internal with only one child
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto *old_root = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_id = old_root->ValueAt(0);

    auto *new_root_page = buffer_pool_manager_->FetchPage(new_root_id);
    auto *new_root = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);

    root_page_id_ = new_root_id;
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(new_root_id, true);
    return true;
  }

  return false;
}

/*
 * Redistribute entries between two nodes
 * @param from_left: true if borrowing from left sibling
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, InternalPage *parent, int index, bool from_left) {
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);

    if (from_left) {
      neighbor_leaf->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    } else {
      neighbor_leaf->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(index + 1, neighbor_leaf->KeyAt(0));
    }
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);

    if (from_left) {
      KeyType middle_key = parent->KeyAt(index);
      neighbor_internal->MoveLastToFrontOf(internal_node, middle_key, buffer_pool_manager_);
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    } else {
      KeyType middle_key = parent->KeyAt(index + 1);
      neighbor_internal->MoveFirstToEndOf(internal_node, middle_key, buffer_pool_manager_);
      parent->SetKeyAt(index + 1, neighbor_internal->KeyAt(0));
    }
  }
}

/*
 * Coalesce (merge) two nodes
 * Move all entries from right node to left node
 * @return true if parent should be deleted
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node, InternalPage *parent, int index, Transaction *transaction)
    -> bool {
  // node is at index, neighbor_node is at index-1 (left sibling)
  KeyType middle_key = parent->KeyAt(index);

  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);
    leaf_node->MoveAllTo(neighbor_leaf);
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);
    internal_node->MoveAllTo(neighbor_internal, middle_key, buffer_pool_manager_);
  }

  // Remove the entry from parent
  parent->Remove(index);

  // Check if parent needs to coalesce or redistribute
  return CoalesceOrRedistribute(parent, transaction);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
  }

  // Find the leftmost leaf page
  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
  }
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page->RLatch();
  root_latch_.RUnlock();

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id = internal->ValueAt(0);
    auto *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    if (child_page == nullptr) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
    }
    child_page->RLatch();
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = child_page;
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  // Release the latch but keep the page pinned for the iterator
  page->RUnlatch();
  return INDEXITERATOR_TYPE(reinterpret_cast<LeafPage *>(node), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
  }

  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
  }
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page->RLatch();
  root_latch_.RUnlock();

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id = internal->Lookup(key, comparator_);
    auto *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    if (child_page == nullptr) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
    }
    child_page->RLatch();
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = child_page;
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  auto *leaf_page = reinterpret_cast<LeafPage *>(node);
  int index = leaf_page->KeyIndex(key, comparator_);
  // Release the latch but keep the page pinned for the iterator
  page->RUnlatch();
  return INDEXITERATOR_TYPE(leaf_page, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
