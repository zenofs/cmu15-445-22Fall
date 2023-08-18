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
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {}

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
  //获取与key对应的bucket
  size_t index = ExtendibleHashTable<K,V>::IndexOf(key);
  std::shared_ptr<Bucket> curBucket = dir_[index];
  //对应bucket查找
  return curBucket->Find(key,value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
    //Todo : 加锁
  std::scoped_lock<std::mutex> lock(latch_);
  //获取与key对应的bucket
  size_t index = ExtendibleHashTable<K,V>::IndexOf(key);
  std::shared_ptr<Bucket> curBucket = dir_[index];
  return curBucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket){
  //拆分成两个bucket
 
  auto curList = bucket->GetItems();
  bucket->IncrementDepth();
  auto curDepth = bucket->GetDepth(); 
  std::shared_ptr<Bucket> bucket1(new Bucket(bucket_size_,curDepth));
  ++num_buckets_;
  size_t preidx = std::hash<K>()((curList.begin())->first) & ((1 << (curDepth - 1)) - 1);
  //根据key和curDepth对bucket进行shuffle
  for(auto i = curList.begin(); i!= curList.end();i++){
    auto key = i->first;
    auto curIndex = std::hash<K>()(key) & ((1 << (curDepth - 1)) - 1);
    if(curIndex != preidx){
      bucket1->Insert(key,i->second);
      curList.erase(i);
    } 
  }
   for (size_t i = 0; i < dir_.size(); i++) {
    // 1xxx
    if ((i & ((1 << (curDepth - 1)) - 1)) == preidx && (i & ((1 << curDepth) - 1)) != preidx) {
      dir_[i] = bucket1;
    }
  }
  return;
}

  // * @brief Insert the given key-value pair into the hash table.
  //  * If a key already exists, the value should be updated.
  //  * If the bucket is full and can't be inserted, do the following steps before retrying:
  //  *    1. If the local depth of the bucket is equal to the global depth,
  //  *        increment the global depth and double the size of the directory.
  //  *    2. Increment the local depth of the bucket.
  //  *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  while(true){
    size_t index = IndexOf(key);
    bool flag = dir_[index]->Insert(key,value);
    if(flag){
      break;
    }
    if(GetLocalDepthInternal(index) != GetGlobalDepthInternal()) {
      RedistributeBucket(dir_[index]);
    } else {
      global_depth_++;
      size_t dir_size = dir_.size();
      for (size_t i = 0; i < dir_size; i++) {
        dir_.emplace_back(dir_[i]);
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
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = list_.begin();
  for(; it != list_.end(); it++){
    if(it->first == key){
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  //  //Todo : 加锁
  auto it = list_.begin();
  for(; it != list_.end(); it++){
    if(it->first == key){
      it->second = value;
      return true;
    }
  }
  if (this->IsFull()) {
    return false;
  }
  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
