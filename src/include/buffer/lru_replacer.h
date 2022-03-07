//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include <memory>
#include "buffer/replacer.h"
#include "common/config.h"
#include <unordered_map>

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;
  void Print();
 private:
  // TODO(student): implement me!
  struct DLinkedNode {
    frame_id_t key;
    DLinkedNode() {}
    DLinkedNode(frame_id_t k):key(k) {}
    std::shared_ptr<DLinkedNode> pre;
    std::shared_ptr<DLinkedNode> next;
  };
  int cap;
  int size;
  std::mutex mutex;
  std::shared_ptr<DLinkedNode> head;
  std::shared_ptr<DLinkedNode> tail;
  std::unordered_map<frame_id_t,std::shared_ptr<DLinkedNode>> hash;
};

}  // namespace bustub
