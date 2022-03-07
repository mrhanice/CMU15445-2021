//===----------------------------------------------------------------------===//
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
    size = 0;
    cap = num_pages;
    head = std::make_shared<DLinkedNode>();
    tail = std::make_shared<DLinkedNode>();
    head->next = tail;
    head->pre = tail;
    tail->next = head;
    tail->pre = head;
    
}

LRUReplacer::~LRUReplacer() {
    while(head) {
        std::shared_ptr<DLinkedNode> temp = head->next;
        head->next = nullptr;
        head = temp;
     }
     head = nullptr;

    while(tail) {
        std::shared_ptr<DLinkedNode> temp = tail->pre;
        tail->pre = nullptr;
        tail = temp;
    }
    tail = nullptr;
}

void LRUReplacer::Print() {
    std::shared_ptr<LRUReplacer::DLinkedNode> pt = head;
    std::shared_ptr<LRUReplacer::DLinkedNode> pd = tail;
    while(pt != nullptr && pt != pd) {
        printf("key = %d\n",pt->key);
        pt = pt->next;
    }
}

// 使用LRU策略删除一个victim frame，frame_id需要赋值
bool LRUReplacer::Victim(frame_id_t *frame_id) {
    std::lock_guard<std::mutex> guard(mutex);
    if(size == 0) {
        *frame_id = -1;
        return false;
    }
    *frame_id = (head->next->key);
    head->next->next->pre = head;
    head->next = head->next->next;
    hash.erase(*frame_id);
    size--;
    return true; 
}


//有线程 pin这个frame, 表明它不应该成为victim（则在replacer中移除该frame_id）
void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(mutex);
    std::unordered_map<frame_id_t,std::shared_ptr<DLinkedNode>>::iterator des = hash.find(frame_id);
    if(des != hash.end()) {
        des->second->pre->next = des->second->next;
        des->second->next->pre = des->second->pre;
        hash.erase(frame_id);
        if(size > 0) size--;
    }
}

//没有线程pin这个frame, 表明它可以成为victim（则将该frame_id添加到replacer）
void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(mutex);
    if(size >= cap) {
        return;
    } 
    std::unordered_map<frame_id_t,std::shared_ptr<DLinkedNode>>::iterator des = hash.find(frame_id);
    if(des == hash.end()) {
        std::shared_ptr<DLinkedNode> temp = std::make_shared<DLinkedNode>(frame_id);
        tail->pre->next = temp;
        temp->pre = tail->pre;
        temp->next = tail;
        tail->pre = temp;
        size++;
        hash.insert(std::pair<frame_id_t,std::shared_ptr<DLinkedNode>>(frame_id,temp));
    }
    
}

size_t LRUReplacer::Size() { return size; }

}  // namespace bustub
