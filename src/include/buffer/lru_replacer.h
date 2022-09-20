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
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>
#include "buffer/replacer.h"
#include "common/config.h"

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
    frame_id_t key_;
    DLinkedNode() = default;
    explicit DLinkedNode(frame_id_t k) : key_(k) {}
    std::shared_ptr<DLinkedNode> pre_;
    std::shared_ptr<DLinkedNode> next_;
  };
  int cap_;
  int size_;
  std::mutex mutex_;
  std::shared_ptr<DLinkedNode> head_;
  std::shared_ptr<DLinkedNode> tail_;
  std::unordered_map<frame_id_t, std::shared_ptr<DLinkedNode>> hash_;
};

}  // namespace bustub
