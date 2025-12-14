/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator()
    : page_id_(INVALID_PAGE_ID), leaf_(nullptr), index_(0), buffer_pool_manager_(nullptr) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage *leaf, int index, BufferPoolManager *buffer_pool_manager)
    : page_id_(leaf != nullptr ? leaf->GetPageId() : INVALID_PAGE_ID),
      leaf_(leaf),
      index_(index),
      buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(IndexIterator &&other) noexcept
    : page_id_(other.page_id_),
      leaf_(other.leaf_),
      index_(other.index_),
      buffer_pool_manager_(other.buffer_pool_manager_) {
  // Take ownership - nullify the source
  other.page_id_ = INVALID_PAGE_ID;
  other.leaf_ = nullptr;
  other.index_ = 0;
  other.buffer_pool_manager_ = nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator=(IndexIterator &&other) noexcept -> IndexIterator & {
  if (this != &other) {
    // Release current resources
    if (leaf_ != nullptr && buffer_pool_manager_ != nullptr) {
      buffer_pool_manager_->UnpinPage(page_id_, false);
    }
    // Take ownership from other
    page_id_ = other.page_id_;
    leaf_ = other.leaf_;
    index_ = other.index_;
    buffer_pool_manager_ = other.buffer_pool_manager_;
    // Nullify source
    other.page_id_ = INVALID_PAGE_ID;
    other.leaf_ = nullptr;
    other.index_ = 0;
    other.buffer_pool_manager_ = nullptr;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_ != nullptr && buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->UnpinPage(page_id_, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  assert(leaf_ != nullptr);
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  // If we've reached the end of the current leaf page
  if (index_ >= leaf_->GetSize()) {
    page_id_t next_page_id = leaf_->GetNextPageId();

    // Unpin current page
    buffer_pool_manager_->UnpinPage(page_id_, false);

    if (next_page_id == INVALID_PAGE_ID) {
      // Reached the end of the B+ tree
      leaf_ = nullptr;
      page_id_ = INVALID_PAGE_ID;
      index_ = 0;  // Reset index to match End() iterator
    } else {
      // Move to the next leaf page
      auto *next_page = buffer_pool_manager_->FetchPage(next_page_id);
      leaf_ = reinterpret_cast<LeafPage *>(next_page->GetData());
      page_id_ = next_page_id;
      index_ = 0;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return page_id_ == itr.page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
