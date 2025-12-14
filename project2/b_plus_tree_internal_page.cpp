//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

/**
 * Helper method to set the value at given index
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

/**
 * Find the index of the child pointer that points to given value (page_id)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); ++i) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

/**
 * Lookup the child pointer (page_id) for given key using binary search
 * Find the rightmost child pointer where key >= K(i)
 * Since first key is invalid, we search from index 1
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // Binary search to find the largest index i such that key >= array_[i].first
  // We search in [1, size) because array_[0].first is invalid
  int left = 1;
  int right = GetSize();

  while (left < right) {
    int mid = left + (right - left) / 2;
    if (comparator(key, array_[mid].first) < 0) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }

  // left is now the first index where key < array_[left].first
  // So we return array_[left-1].second
  return array_[left - 1].second;
}

/**
 * Populate new root page with old_value + new_key & new_value
 * Called when the root splits and we need a new root
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}

/**
 * Insert new_key & new_value pair right after the pair with old_value
 * @return size after insert
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> int {
  int idx = ValueIndex(old_value) + 1;

  // Shift elements to make room
  for (int i = GetSize(); i > idx; --i) {
    array_[i] = array_[i - 1];
  }

  // Insert new key-value pair
  array_[idx].first = new_key;
  array_[idx].second = new_value;
  IncreaseSize(1);

  return GetSize();
}

/**
 * Move half of items to recipient (for split)
 * @param recipient: the new internal page created from split
 * @param buffer_pool_manager: used to update parent pointers of moved children
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int start_idx = GetSize() / 2;
  int move_count = GetSize() - start_idx;

  recipient->CopyNFrom(array_ + start_idx, move_count, buffer_pool_manager);
  IncreaseSize(-move_count);
}

/**
 * Copy items into this internal page from source
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  int start = GetSize();
  for (int i = 0; i < size; ++i) {
    array_[start + i] = items[i];
    // Update parent pointer of the child page
    auto *page = buffer_pool_manager->FetchPage(items[i].second);
    auto *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(items[i].second, true);
  }
  IncreaseSize(size);
}

/**
 * Remove key & value pair at given index
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // Shift elements to fill the gap
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/**
 * Move all items to recipient (for merge)
 * @param middle_key: the key that was in parent pointing to this page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // The first key of this page is invalid, replace it with middle_key
  array_[0].first = middle_key;

  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/**
 * Move first item to end of recipient
 * @param middle_key: the key that was in parent between recipient and this page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  MappingType pair{middle_key, array_[0].second};
  recipient->CopyLastFrom(pair, buffer_pool_manager);

  // Shift remaining elements (but keep array_[0].first invalid for internal page)
  for (int i = 0; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/**
 * Append an entry at the end of this internal page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array_[GetSize()] = pair;

  // Update parent pointer of the child page
  auto *page = buffer_pool_manager->FetchPage(pair.second);
  auto *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);

  IncreaseSize(1);
}

/**
 * Move last item to front of recipient
 * @param middle_key: the key that was in parent between this page and recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  MappingType pair{middle_key, array_[GetSize() - 1].second};
  recipient->CopyFirstFrom(pair, buffer_pool_manager);
  IncreaseSize(-1);
}

/**
 * Insert an entry at the front of this internal page (shift existing elements)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  for (int i = GetSize(); i > 0; --i) {
    array_[i] = array_[i - 1];
  }
  array_[0] = pair;

  // Update parent pointer of the child page
  auto *page = buffer_pool_manager->FetchPage(pair.second);
  auto *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);

  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
