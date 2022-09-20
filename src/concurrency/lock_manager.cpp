// ===----------------------------------------------------------------------===//
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
#include <utility>
#include <vector>
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);

recheckAfterNotify:

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 如果隔离级别是READ_UNCOMMITTED，不用读锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // REPEATABLE_READ, SHRINKING不可获得锁,REPEATABLE_READ是严格遵守两阶段协议的
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 已经获得锁
  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  LockRequestQueue &rq = lock_table_[rid];
  if (lock_table_.count(rid) != 0U) {
    auto it = rq.request_queue_.begin();
    // bool haveInsert = false;
    while (it != rq.request_queue_.end()) {
      // 当前事务是老事务，队列中已经有的是新事物，根据锁相容，如果新事物都是读锁，则相容，写锁的话，不相容
      // 等待队列中的新事物持有写锁，根据wound-wait,需要Abort这个事务
      Transaction *tra = TransactionManager::GetTransaction(it->txn_id_);
      if (it->txn_id_ > txn->GetTransactionId() && (it->lock_mode_ == LockMode::EXCLUSIVE)) {
        // Abort掉这个新事物
        tra->GetExclusiveLockSet()->erase(rid);
        tra->SetState(TransactionState::ABORTED);
        it = rq.request_queue_.erase(it);
      } else if (it->txn_id_ < txn->GetTransactionId() && (it->lock_mode_ == LockMode::EXCLUSIVE)) {
        // 当前事务是新事物，如果有老事务已经持有写锁，则等待
        rq.cv_.wait(ul);  //
        goto recheckAfterNotify;
      } else {
        it++;
      }
    }
  }

  txn->SetState(TransactionState::GROWING);
  LockRequest new_add(txn->GetTransactionId(), LockMode::SHARED);
  new_add.granted_ = true;
  rq.request_queue_.emplace_back(new_add);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);

  // recheckAfterNotify:

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // REPEATABLE_READ, SHRINKING不可获得锁,REPEATABLE_READ是严格遵守两阶段协议的
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 已经获得锁
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  LockRequestQueue &rq = lock_table_[rid];
  if (lock_table_.count(rid) != 0U) {
    auto it = rq.request_queue_.begin();
    while (it != rq.request_queue_.end()) {
      Transaction *tra = TransactionManager::GetTransaction(it->txn_id_);
    
      if (it->txn_id_ > txn->GetTransactionId()) {
        // 当前事务是老事务，abort掉在等待队列中的新事务
        tra->SetState(TransactionState::ABORTED);
        if (it->lock_mode_ == LockMode::SHARED) {
          tra->GetSharedLockSet()->erase(rid);
        } else {
          tra->GetExclusiveLockSet()->erase(rid);
        }
        it = rq.request_queue_.erase(it);
      } else if (it->txn_id_ < txn->GetTransactionId()) {
        // 当前事务是新事务，等待老事务释放锁
        txn->SetState(TransactionState::ABORTED);
        return false;
        // rq.cv_.wait(ul);
        // goto recheckAfterNotify;
      } else {
        it++;
      }
    }
  }

  txn->SetState(TransactionState::GROWING);
  LockRequest new_add(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  new_add.granted_ = true;
  rq.request_queue_.emplace_back(new_add);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);

recheckAfterNotify:

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // REPEATABLE_READ, SHRINKING不可获得锁,REPEATABLE_READ是严格遵守两阶段协议的
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 如果事务本身并没有获取到共享锁，则返回false
  if (!txn->IsSharedLocked(rid)) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 如果已经获得排它锁，则直接返回true
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  LockRequestQueue &rq = lock_table_[rid];
  auto it = rq.request_queue_.begin();

  while (it != rq.request_queue_.end()) {
    Transaction *tra = TransactionManager::GetTransaction(it->txn_id_);
    if (it->txn_id_ > txn->GetTransactionId()) {
      // 当前事务是老事务, Abort掉新事物
      if (it->lock_mode_ == LockMode::SHARED) {
        tra->GetSharedLockSet()->erase(rid);
      } else {
        tra->GetExclusiveLockSet()->erase(rid);
      }
      it = rq.request_queue_.erase(it);
      tra->SetState(TransactionState::ABORTED);
    } else if (it->txn_id_ < txn->GetTransactionId()) {
      // 当前是新事物，等待
      rq.cv_.wait(ul);
      goto recheckAfterNotify;
    } else {
      it++;
    }
  }
  // 此时老事务已经Abort完了，所有新事物应该都等待完毕，与LockExclusive类似，LockExeclusive是获取锁时，一定是队列中第一个
  // LockUpdate此时队列中应该只剩下其自身的lockShared请求
  auto &request_shard = rq.request_queue_.front();
  // std::cout << request_shard.txn_id_ << " " << txn->GetTransactionId() << std::endl;
  assert(request_shard.txn_id_ == txn->GetTransactionId());
  request_shard.lock_mode_ = LockMode::EXCLUSIVE;
  request_shard.granted_ = true;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  txn->SetState(TransactionState::GROWING);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);

  // 如果隔离级别是repeatable_read，设置为shrinking阶段
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  LockRequestQueue &rq = lock_table_[rid];
  auto it = rq.request_queue_.begin();
  bool ok = false;
  while (it != rq.request_queue_.end()) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      LockMode mode = it->lock_mode_;
      it = rq.request_queue_.erase(it);
      if (mode == LockMode::SHARED) {
        txn->GetSharedLockSet()->erase(rid);
        rq.cv_.notify_all();
        ok = true;
      } else {
        txn->GetExclusiveLockSet()->erase(rid);
        rq.cv_.notify_all();
        ok = true;
      }
    } else {
      it++;
    }
  }
  return ok;
}

}  // namespace bustub
