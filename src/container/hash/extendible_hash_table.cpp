// ===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
// ===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"
#define MAX_GLOBAL_DEPTH 9

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  directory_page_id_ = INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

/* 获取HashTableDirectoryPage,如果没有,首先创建，如果有则直接获取
   从buffer_pool_manager中获取Page,Page中是一个Directory对象
*/
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *dir_page;
  directory_lock_.lock();
  if (directory_page_id_ == INVALID_PAGE_ID) {
    page_id_t page_id_dir;
    Page *page = buffer_pool_manager_->NewPage(&page_id_dir);
    dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
    directory_page_id_ = page_id_dir;
    dir_page->SetPageId(directory_page_id_);

    page_id_t page_id_bucket;
    page = buffer_pool_manager_->NewPage(&page_id_bucket);
    dir_page->SetBucketPageId(0, page_id_bucket);
    buffer_pool_manager_->UnpinPage(page_id_dir, true);
    buffer_pool_manager_->UnpinPage(page_id_bucket, true);
  }
  directory_lock_.unlock();
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  return page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 * 根据key获取数据
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();  // Readers includes inserts and removes
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, dir_page);
  Page *page = FetchPage(page_id);
  page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page);
  bool ok = bucket_page->GetValue(key, comparator_, result);
  page->RUnlatch();

  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(page_id, false);
  table_latch_.RUnlock();
  return ok;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, dir_page);
  Page *page = FetchPage(page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page);
  if (!bucket_page->IsFull()) {
    bool ok = bucket_page->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, true);
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    table_latch_.RUnlock();
    return ok;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, false);
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();  // writers are splits and merges
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_id = KeyToDirectoryIndex(key, dir_page);
  uint32_t bucket_local_depth = dir_page->GetLocalDepth(bucket_id);

  // hash表已经不能再扩容了
  if (bucket_local_depth >= MAX_GLOBAL_DEPTH) {
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    table_latch_.WUnlock();
    return false;
  }

  // hash表可以再扩容，但是bucket_local_depth == global_depth
  // 需要首先扩容Directory表
  if (bucket_local_depth == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  // 先增加local_depth,这一步很关键，想想逻辑
  dir_page->IncrLocalDepth(bucket_id);

  // 更新old bucket的信息
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *old_page = FetchPage(bucket_page_id);
  old_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *old_bucket = FetchBucketPage(old_page);
  uint32_t num = old_bucket->NumReadable();
  MappingType *temp_old_pairs = old_bucket->GetMappingTypeArray();
  old_bucket->Init();

  // 创建一个新的bucket
  page_id_t image_page_id;
  Page *image_page = buffer_pool_manager_->NewPage(&image_page_id);
  assert(image_page != nullptr);
  image_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *image_bucket = FetchBucketPage(image_page);
  uint32_t split_image_index = dir_page->GetSplitImageIndex(bucket_id);
  dir_page->SetLocalDepth(split_image_index, dir_page->GetLocalDepth(bucket_id));
  dir_page->SetBucketPageId(split_image_index, image_page_id);

  for (uint32_t i = 0; i < num; i++) {
    uint32_t new_bucket_id = Hash(temp_old_pairs[i].first) & dir_page->GetLocalDepthMask(bucket_id);
    page_id_t temp_page_id = dir_page->GetBucketPageId(new_bucket_id);
    assert(temp_page_id == bucket_page_id || temp_page_id == image_page_id);
    if (temp_page_id == bucket_page_id) {
      old_bucket->Insert(temp_old_pairs[i].first, temp_old_pairs[i].second, comparator_);
    } else {
      image_bucket->Insert(temp_old_pairs[i].first, temp_old_pairs[i].second, comparator_);
    }
  }
  delete[] temp_old_pairs;

  // 上面只修改了原bucket与image_bucket的相关信息，
  // 实际上可能之前存在许多bucket映射到bucket对应的page上,这些信息也要相应的修改
  uint32_t step = 1 << (dir_page->GetLocalDepth(bucket_id));
  for (uint32_t i = bucket_id; i >= step; i -= step) {
    dir_page->SetBucketPageId(i, bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_id));
  }
  for (uint32_t i = bucket_id; i < dir_page->Size(); i += step) {
    dir_page->SetBucketPageId(i, bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_id));
  }
  for (uint32_t i = split_image_index; i >= step; i -= step) {
    dir_page->SetBucketPageId(i, image_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_image_index));
  }
  for (uint32_t i = split_image_index; i < dir_page->Size(); i += step) {
    dir_page->SetBucketPageId(i, image_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_image_index));
  }

  old_page->WUnlatch();
  image_page->WUnlatch();

  // Unpin 这三页数据
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(image_page_id, true);
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  table_latch_.WUnlock();

  // 再次尝试插入数据
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();  // Readers includes inserts and removes
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_id = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_id);
  Page *page = FetchPage(bucket_page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(page);
  bool ok = bucket->Remove(key, value, comparator_);

  // 如果当前bucket空了，则执行合并
  if (bucket->IsEmpty()) {
    page->WUnlatch();
    // Unpin
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    table_latch_.RUnlock();
    Merge(transaction, key, value);
    return ok;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  table_latch_.RUnlock();
  return ok;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();  // writers are splits and merges
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_id = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_id);
  uint32_t image_bucket_id = dir_page->GetSplitImageIndex(bucket_id);

  // local depth为0说明已经最小了，不收缩
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_id);
  if (local_depth == 0) {
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    table_latch_.WUnlock();
    return;
  }

  // 如果该bucket与其split image深度不同，也不收缩
  if (local_depth != dir_page->GetLocalDepth(image_bucket_id)) {
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    table_latch_.WUnlock();
    return;
  }

  // 下面之所以在检查一遍是否为空是因为并发执行的原因，在上一个函数已经完全释放了锁
  // 当执行到此处时，其他线程可能已经修改了此bucket，导致此时bucket不为空了，所以需要再检查一遍
  Page *bucket_page = FetchPage(bucket_page_id);
  bucket_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page);
  if (!bucket->IsEmpty()) {
    bucket_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    table_latch_.WUnlock();
    return;
  }

  bucket_page->RUnlatch();
  // 删除bucket，此时该bucket已经为空
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->DeletePage(bucket_page_id);

  // 执行合并
  page_id_t image_page_id = dir_page->GetBucketPageId(image_bucket_id);
  dir_page->SetBucketPageId(bucket_id, image_page_id);
  dir_page->DecrLocalDepth(bucket_id);
  dir_page->DecrLocalDepth(image_bucket_id);
  assert(dir_page->GetLocalDepth(bucket_id) == dir_page->GetLocalDepth(image_bucket_id));

  // 遍历整个directory，将所有指向bucket_page的bucket全部重新指向image_bucket_page
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    page_id_t temp_page_id = dir_page->GetBucketPageId(i);
    if (temp_page_id == bucket_page_id || temp_page_id == image_page_id) {
      dir_page->SetBucketPageId(i, image_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(image_bucket_id));
    }
  }

  // 判断global_depth是否需要缩减
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
