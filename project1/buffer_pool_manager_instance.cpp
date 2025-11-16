//===----------------------------------------------------------------------===//
//
//                          BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // ** 移除了 `throw NotImplementedException` **
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  bool found_frame = false;

  // 1. 尝试从 free_list_ 获取
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    found_frame = true;
  } else {
    // 2. 尝试从 replacer_ 驱逐
    if (replacer_->Evict(&frame_id)) {
      found_frame = true;
      page_id_t old_page_id = pages_[frame_id].GetPageId();

      // 2a. 如果是脏页，写回磁盘
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(old_page_id, pages_[frame_id].GetData());
        pages_[frame_id].is_dirty_ = false;
      }
      // 2b. 从 page_table_ 移除
      page_table_->Remove(old_page_id);
    }
  }

  // 3. 如果没有空闲帧 (所有帧都被 pin)，返回 nullptr
  if (!found_frame) {
    return nullptr;
  }

  // 4. 分配新 page_id 并设置新页
  *page_id = AllocatePage();

  // 5. 更新元数据和 Page 对象
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);  // 新页/获取的页默认不可驱逐

  pages_[frame_id].ResetMemory();
  // pages_[frame_id].SetPageId(*page_id); // 错误行
  pages_[frame_id].page_id_ = *page_id;  // ** 修正 **
  pages_[frame_id].pin_count_ = 1;         // Pin 计数为 1
  pages_[frame_id].is_dirty_ = false;     // 新页是干净的

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;

  // 1. 尝试在 page_table_ 中查找
  if (page_table_->Find(page_id, frame_id)) {
    // 页面在缓冲池中
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);  // Pin 住，不可驱逐
    return &pages_[frame_id];
  }

  // 2. 页面不在缓冲池中，需要获取一个帧
  bool found_frame = false;
  // 2a. 尝试从 free_list_ 获取
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    found_frame = true;
  } else {
    // 2b. 尝试从 replacer_ 驱逐
    if (replacer_->Evict(&frame_id)) {
      found_frame = true;
      page_id_t old_page_id = pages_[frame_id].GetPageId();

      // 如果是脏页，写回磁盘
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(old_page_id, pages_[frame_id].GetData());
        pages_[frame_id].is_dirty_ = false;
      }
      // 从 page_table_ 移除
      page_table_->Remove(old_page_id);
    }
  }

  // 3. 如果没有可用的帧 (所有帧都被 pin)，返回 nullptr
  if (!found_frame) {
    return nullptr;
  }

  // 4. 从磁盘读取页面到帧中
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  // 5. 更新元数据和 Page 对象
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // pages_[frame_id].SetPageId(page_id); // 错误行
  pages_[frame_id].page_id_ = page_id;  // ** 修正 **
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  // 检查页是否在缓冲池中
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  // 检查 pin_count
  if (pages_[frame_id].GetPinCount() == 0) {
    return false;
  }

  // 减少 pin_count
  pages_[frame_id].pin_count_--;

  // 更新 dirty 标志
  // 只有当调用者标记为 dirty 时才更新。如果它已经是 dirty，保持 dirty。
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }

  // 如果 pin_count 降为 0，设置其为可驱逐
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  // 检查页是否在缓冲池中
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  // 将页面数据写回磁盘
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());

  // 刷新后，页面不再是 dirty
  pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);

  // 遍历所有帧
  for (size_t i = 0; i < pool_size_; ++i) {
    page_id_t page_id = pages_[i].GetPageId();
    // 如果帧中有一个有效的页面
    if (page_id != INVALID_PAGE_ID) {
      // 强制刷新
      disk_manager_->WritePage(page_id, pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  // 1. 检查页是否在缓冲池中
  if (page_table_->Find(page_id, frame_id)) {
    // 2. 如果在缓冲池中，检查 pin 计数
    if (pages_[frame_id].GetPinCount() > 0) {
      // 页面正在被使用，无法删除
      return false;
    }
    // 3. 从缓冲池中移除
    page_table_->Remove(page_id);
    replacer_->Remove(frame_id);      // 从 replacer 移除
    free_list_.push_back(frame_id);  // 归还到 free_list

    // 重置 Page 对象元数据
    pages_[frame_id].ResetMemory();
    // pages_[frame_id].SetPageId(INVALID_PAGE_ID); // 错误行
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;  // ** 修正 **
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].is_dirty_ = false;
  }

  // 4. 不管页是否在缓冲池中，都告诉 disk_manager 释放该页
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
