// ===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  size_ = 0;
  cap_ = num_pages;
  head_ = std::make_shared<DLinkedNode>();
  tail_ = std::make_shared<DLinkedNode>();
  head_->next_ = tail_;
  head_->pre_ = tail_;
  tail_->next_ = head_;
  tail_->pre_ = head_;
}

LRUReplacer::~LRUReplacer() {
  while (head_) {
    std::shared_ptr<DLinkedNode> temp = head_->next_;
    head_->next_ = nullptr;
    head_ = temp;
  }
  head_ = nullptr;

  while (tail_) {
    std::shared_ptr<DLinkedNode> temp = tail_->pre_;
    tail_->pre_ = nullptr;
    tail_ = temp;
  }
  tail_ = nullptr;
}

void LRUReplacer::Print() {
  std::shared_ptr<LRUReplacer::DLinkedNode> pt = head_;
  std::shared_ptr<LRUReplacer::DLinkedNode> pd = tail_;
  while (pt != nullptr && pt != pd) {
    printf("key = %d\n", pt->key_);
    pt = pt->next_;
  }
}

// 使用LRU策略删除一个victim frame，frame_id需要赋值
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (size_ == 0) {
    *frame_id = -1;
    return false;
  }
  *frame_id = (head_->next_->key_);
  head_->next_->next_->pre_ = head_;
  head_->next_ = head_->next_->next_;
  hash_.erase(*frame_id);
  size_--;
  return true;
}

// 有线程 pin这个frame, 表明它不应该成为victim（则在replacer中移除该frame_id）
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  std::unordered_map<frame_id_t, std::shared_ptr<DLinkedNode>>::iterator des = hash_.find(frame_id);
  if (des != hash_.end()) {
    des->second->pre_->next_ = des->second->next_;
    des->second->next_->pre_ = des->second->pre_;
    hash_.erase(frame_id);
    if (size_ > 0) {
      size_--;
    }
  }
}

// 没有线程pin这个frame, 表明它可以成为victim（则将该frame_id添加到replacer）
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (size_ >= cap_) {
    return;
  }
  std::unordered_map<frame_id_t, std::shared_ptr<DLinkedNode>>::iterator des = hash_.find(frame_id);
  if (des == hash_.end()) {
    std::shared_ptr<DLinkedNode> temp = std::make_shared<DLinkedNode>(frame_id);
    tail_->pre_->next_ = temp;
    temp->pre_ = tail_->pre_;
    temp->next_ = tail_;
    tail_->pre_ = temp;
    size_++;
    hash_.insert(std::pair<frame_id_t, std::shared_ptr<DLinkedNode>>(frame_id, temp));
  }
}

size_t LRUReplacer::Size() { return size_; }

}  // namespace bustub
