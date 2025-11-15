//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  // 初始化时创建一个桶，全局深度为0
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  
  while (true) {
    size_t index = IndexOf(key);
    auto target_bucket = dir_[index];
    
    // 尝试插入到桶中
    if (target_bucket->Insert(key, value)) {
      return;  // 插入成功
    }
    
    // 桶已满，需要分裂
    int local_depth = target_bucket->GetDepth();
    int global_depth = GetGlobalDepthInternal();
    
    // 如果局部深度等于全局深度，需要先增加全局深度
    if (local_depth == global_depth) {
      global_depth_++;
      size_t dir_size = dir_.size();
      // 扩展目录，每个现有条目都复制一份
      for (size_t i = 0; i < dir_size; i++) {
        dir_.push_back(dir_[i]);
      }
    }
    
    // 增加局部深度并创建新桶
    target_bucket->IncrementDepth();
    local_depth++;
    
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, local_depth);
    num_buckets_++;
    
    // 重新分配桶中的元素
    auto &items = target_bucket->GetItems();
    std::list<std::pair<K, V>> temp_items(items.begin(), items.end());
    items.clear();
    
    for (const auto &item : temp_items) {
      size_t hash_value = std::hash<K>()(item.first);
      size_t bucket_index = hash_value & ((1 << local_depth) - 1);
      
      if ((bucket_index & (1 << (local_depth - 1))) == 0) {
        // 留在原桶
        target_bucket->GetItems().push_back(item);
      } else {
        // 移到新桶
        new_bucket->GetItems().push_back(item);
      }
    }
    
    // 更新目录中的指针
    // 找到所有之前指向 target_bucket 的目录项，并根据新的深度重新分配
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket) {
        // 根据索引 i 的第 (local_depth-1) 位来决定指向哪个桶
        if ((i >> (local_depth - 1)) & 1) {
          dir_[i] = new_bucket;
        }
        // 否则保持指向 target_bucket
      }
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &pair : list_) {
    if (pair.first == key) {
      value = pair.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // 检查键是否已存在，如果存在则更新值
  for (auto &pair : list_) {
    if (pair.first == key) {
      pair.second = value;
      return true;
    }
  }
  
  // 检查桶是否已满
  if (IsFull()) {
    return false;
  }
  
  // 插入新的键值对
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub



// 实现总结

// 1. 构造函数
// - 初始化时创建一个深度为0的桶

// 2. 核心方法

// Find(K, V): 
// - 通过 IndexOf 计算键的哈希索引
// - 在对应的桶中查找键值对

// Insert(K, V): 
// - 尝试插入到对应的桶
// - 如果桶满了：
//   - 检查是否需要增加全局深度（当局部深度=全局深度时）
//   - 如果需要，扩展目录大小（加倍）
//   - 增加桶的局部深度
//   - 创建新桶并分裂元素
//   - 更新目录指针

// Remove(K):
// - 找到对应的桶并删除键值对

// 3. Bucket 类方法
// - Find: 遍历链表查找键
// - Insert: 检查键是否存在（更新）或桶是否已满，然后插入
// - Remove: 遍历链表删除键值对

// 4. 线程安全
// - 所有公共方法都使用 std::scoped_lock<std::mutex> 保护

