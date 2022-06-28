//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);

  recheckAfterNotify:
  
  if(txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 如果隔离级别是READ_UNCOMMITTED，不用读锁
  if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // REPEATABLE_READ, SHRINKING不可获得锁,REPEATABLE_READ是严格遵守两阶段协议的
  if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 已经获得锁
  if(txn->IsSharedLocked(rid)) {
    return true;  
  }
  
  LockRequestQueue &rq = lock_table_[rid];
  if(lock_table_.count(rid)) {
    auto it = rq.request_queue_.begin();
    // bool haveInsert = false;
    while(it != rq.request_queue_.end()) {
      // 当前事务是老事务，队列中已经有的是新事物，根据锁相容，如果新事物都是读锁，则相容，写锁的话，不相容
      // 等待队列中的新事物持有写锁，根据wound-wait,需要Abort这个事务
      Transaction *tra = TransactionManager::GetTransaction(it->txn_id_);
      if(it->txn_id_ > txn->GetTransactionId() && (it->lock_mode_ == LockMode::EXCLUSIVE && it->granted_ == true)) {
        
        // Abort掉这个新事物
        tra->GetExclusiveLockSet()->erase(rid); 
        tra->SetState(TransactionState::ABORTED);
        it = rq.request_queue_.erase(it);
      } else if(it->txn_id_ < txn->GetTransactionId() && (it->lock_mode_ == LockMode::EXCLUSIVE)) {
        // 当前事务是新事物，如果有老事务已经持有写锁，则等待
        // LockRequest newAdd(txn->GetTransactionId(),LockMode::SHARED);
        // newAdd.granted_ = false;
        // rq.request_queue_.emplace_back(newAdd);
        // haveInsert = true;
        // txn->GetSharedLockSet()->emplace(rid);
        rq.cv_.wait(ul); // 
        goto recheckAfterNotify;
      } else {
        it++;
      }
    }
  } 
  LockRequest newAdd(txn->GetTransactionId(),LockMode::SHARED);
  newAdd.granted_ = true;
  rq.request_queue_.emplace_back(newAdd);
  txn->GetSharedLockSet()->emplace(rid);
  // if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::GROWING);
  // }
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);
  
  recheckAfterNotify:

  if(txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // REPEATABLE_READ, SHRINKING不可获得锁,REPEATABLE_READ是严格遵守两阶段协议的
  if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 已经获得锁
  if(txn->IsExclusiveLocked(rid)) {
    return true;  
  }
  
  LockRequestQueue &rq = lock_table_[rid];
  if(lock_table_.count(rid)) {
    auto it = rq.request_queue_.begin();
    // bool haveInsert = false;
    while(it != rq.request_queue_.end()) {
      Transaction *tra = TransactionManager::GetTransaction(it->txn_id_);
      if(it->txn_id_ > txn->GetTransactionId() && it->granted_ == true) {
        // 当前事务是老事务，abort掉在等待队列中的新事务
        tra->SetState(TransactionState::ABORTED);
        if(it->lock_mode_ == LockMode::SHARED) {
          tra->GetSharedLockSet()->erase(rid);
        } else {
          tra->GetExclusiveLockSet()->erase(rid);
        }
        it = rq.request_queue_.erase(it);
      } else if(it->txn_id_ < txn->GetTransactionId()) {
        // 当前事务是新事务，等待老事务释放锁
        rq.cv_.wait(ul); // 
        goto recheckAfterNotify;
      } else {
        it++;
      }
    }
  } 

  LockRequest newAdd(txn->GetTransactionId(),LockMode::EXCLUSIVE);
  newAdd.granted_ = true;
  rq.request_queue_.emplace_back(newAdd);
  txn->GetExclusiveLockSet()->emplace(rid);
  // if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::GROWING);
  // }
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);
  
  if(txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // REPEATABLE_READ, SHRINKING不可获得锁,REPEATABLE_READ是严格遵守两阶段协议的
  if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 如果事务本身并没有获取到共享锁，则返回false
  if(!txn->GetSharedLockSet()->count(rid)) {
    return false;
  }

  // 如果已经获得排它锁，则直接返回true
  if(txn->IsExclusiveLocked(rid)) {
    return true;  
  }

  LockRequestQueue &rq = lock_table_[rid];
  auto it = rq.request_queue_.begin();
  while(it != rq.request_queue_.end()) {
    if(it->txn_id_ == txn->GetTransactionId() && it->lock_mode_ == LockMode::SHARED && it->granted_ == true) {
      break;
    }
  }
  if(it == rq.request_queue_.end()) {
    return false;
  } else {
    // 删除之前的share_lock请求
    rq.request_queue_.erase(it);
    txn->GetSharedLockSet()->erase(rid);
    
    // 升级
    recheckAfterNotify:
    auto st = rq.request_queue_.begin();
    while(st != rq.request_queue_.end()) {
      Transaction *tra = TransactionManager::GetTransaction(st->txn_id_);
      if(st->txn_id_ > txn->GetTransactionId() && st->granted_ == true) {
        // 当前事务是老事务，abort掉在等待队列中的新事务
        tra->SetState(TransactionState::ABORTED);
        if(st->lock_mode_ == LockMode::SHARED) {
          tra->GetSharedLockSet()->erase(rid);
        } else {
          tra->GetExclusiveLockSet()->erase(rid);
        }
        it = rq.request_queue_.erase(st);
      } else if(st->txn_id_ < txn->GetTransactionId()) {
        // 当前事务是新事务，等待老事务释放锁
        rq.cv_.wait(ul); // 
        goto recheckAfterNotify;
      } else {
        st++;
      }
    }
    LockRequest newAdd(txn->GetTransactionId(),LockMode::EXCLUSIVE);
    newAdd.granted_ = true;
    rq.request_queue_.emplace_back(newAdd);
    txn->GetExclusiveLockSet()->emplace(rid);
    txn->SetState(TransactionState::GROWING);
    return true;
  }
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);

  // 如果隔离级别是repeatable_read，设置为shrinking阶段
  if(txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  LockRequestQueue &rq = lock_table_[rid];
  auto it = rq.request_queue_.begin();
  while(it != rq.request_queue_.end()) {
    if(it->txn_id_ == txn->GetTransactionId()) {
      LockMode mode = it->lock_mode_;
      it = rq.request_queue_.erase(it);
      if(mode == LockMode::SHARED) {
        txn->GetSharedLockSet()->erase(rid);
        rq.cv_.notify_all();
        return true;
      } else {
        txn->GetExclusiveLockSet()->erase(rid);
        rq.cv_.notify_all();
        return true;
      }
    } else {
      it++;
    }
  }
  return false;
}

}  // namespace bustub
