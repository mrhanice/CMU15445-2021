// ===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));  // 在列表的末尾插入一个新元素
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

/**
 * 将页的内容刷到磁盘
 * 如果此页没有数据，则返回false
 * 如果此页数据是脏的，则刷盘，返回true
 */
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> guard(latch_);
  assert(page_id != INVALID_PAGE_ID);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t ft = it->second;
  Page *page = &pages_[ft];
  if (page->is_dirty_) {
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }
  return true;
}

// 页表的数据全部刷盘
void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> guard(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    Page *page = &pages_[i];
    if (page->page_id_ != INVALID_PAGE_ID && page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->GetData());
      page->is_dirty_ = false;
    }
  }
}

bool BufferPoolManagerInstance::FindFreePage(frame_id_t *ft) {
  if (!free_list_.empty()) {
    *ft = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  if (!replacer_->Victim(ft)) {
    return false;
  }
  Page *page = &pages_[*ft];
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->GetData());
    page->is_dirty_ = false;
  }
  page_table_.erase(page->page_id_);
  return true;
}

/**
 * 这个特别容易出错
 * 1.创建一个新page，首先在bufferpool中找到一个可以防止page的位置(frame_id)
 *  1.1 优先选择free list里是否有空闲frameid,如果没有，看是否可以换出
 *  1.2 这里处理换出需要进行维护：对于要换出的page 刷盘，删除页表项，清空page内存.pin_count=0
 * 2. ok现在已经有可以安放新page的frame_id了，需要维护新的page相关信息
 *  2.1 获取page_id (AllocatePage),在页表中添加(page_id,frame_id)页表项，清空page内存
 *  2.2 pin_count = 1,从LRU中剔除该frame_id
 * */
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t ft = -1;
  bool ok = FindFreePage(&ft);
  if (!ok) {
    return nullptr;
  }
  *page_id = AllocatePage();
  Page *page = &pages_[ft];
  page_table_.emplace(*page_id, ft);
  page->ResetMemory();
  page->page_id_ = *page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  replacer_->Pin(ft);
  return page;
}

// 思路同上，结合下面，按照下面注释来写即可
Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  // 此page在buffer_pool中
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t ft = it->second;
    Page *page = &pages_[ft];
    replacer_->Pin(ft);
    page->pin_count_++;
    return page;
  }
  // 此page不在buffer_pool中,说明在磁盘上，此时首先需要在页表中找一个页号（其实就是frame_id），然后将磁盘数据加载到Page里
  // 并维护好页表
  frame_id_t ft = -1;
  bool is_ft = FindFreePage(&ft);
  if (!is_ft) {
    return nullptr;
  }
  Page *page = &pages_[ft];
  if (page_id != INVALID_PAGE_ID) {
    page_table_.emplace(page_id, ft);
    page->page_id_ = page_id;
    disk_manager_->ReadPage(page_id, page->data_);
    replacer_->Pin(ft);
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    return page;
  }
  return nullptr;
}

/**
 * 如果page不在page_table中返回true
 * 如果pin_count > 0,说明还有线程在占用，返回false
 */
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  // 如果此页不在页表里，返回true
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  // 在页表里，但是有线程在占用，不能删除，返回false
  frame_id_t ft = it->second;
  Page *page = &pages_[ft];
  if (page->pin_count_ > 0) {
    return false;
  }
  // 在页表里，且没有线程占用，可以删除页表里此页的数据
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->GetData());
    page->is_dirty_ = false;
  }
  DeallocatePage(page_id);      // 释放磁盘空间
  page_table_.erase(page_id);   // 页表里删除该页
  free_list_.emplace_back(ft);  // 添加到空闲页表list中
  // 重置元数据
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  return true;
}

/**
 * 某个线程unpin某个page产生什么影响
 * 1. 首先判断page_table中是否有这个page
 *  1.1 如果没有直接返回false
 *  1.2 如果有，获取到该page
 * 2. 获取到page后，page->pin_count--,但是事先应该判断pin_count是否已经是0了
 *  2.1 已经是0，返回false
 *  2.2 page_count--,如果page_count=0，将该frame_id加入到LRU_replacer中,表示该page作为待换出
 * */
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t ft = it->second;
  Page *page = &pages_[ft];
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  if (page->pin_count_ == 0) {
    return false;
  }
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->Unpin(ft);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
