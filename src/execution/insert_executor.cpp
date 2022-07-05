// ===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_heap_(nullptr), child_executor_(move(child_executor)) {}

void InsertExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
  catalog_ = exec_ctx_->GetCatalog();
  tableinfo_ = catalog_->GetTable(plan_->TableOid());
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    for (const auto &row_value : plan_->RawValues()) {
      insert_tuples_.emplace_back(Tuple(row_value, &(tableinfo_->schema_)));
    }
  } else {
    child_executor_->Init();
    Tuple temp_tuple;
    RID temp_rid;
    while (child_executor_->Next(&temp_tuple, &temp_rid)) {
      insert_tuples_.emplace_back(temp_tuple);
    }
  }
  for (auto &insert_row : insert_tuples_) {
    InsertIntoTableWithIndex(&insert_row);
  }
  return false;
}

// 注意写操作的时候要更新事务的写集，以及索引写集，以为了undo
void InsertExecutor::InsertIntoTableWithIndex(Tuple *tuple) {
  RID new_rid;
  // 有个疑问没解决，插入时因为无法拿到rid没法加锁，但是在后面更新索引的时候，需要加锁，那怎么保证的整个过程是原子性的呢？
  // 例如在table_heap_->InsertTuple，与下面加锁前有对tuple的update操作，那下面索引与更新够的值对应不上了
  bool okinsert = table_heap_->InsertTuple(
      *tuple, &new_rid, exec_ctx_->GetTransaction());  // table_write_set由table_heap_->InsertTuple来维护

  // 加锁
  LockManager *lock_manager = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  if (lock_manager != nullptr) {
    if (txn->IsSharedLocked(new_rid)) {
      lock_manager->LockUpgrade(txn, new_rid);
    } else {
      lock_manager->LockExclusive(txn, new_rid);
    }
  }

  if (okinsert) {
    for (auto &indexinfo : catalog_->GetTableIndexes(tableinfo_->name_)) {
      indexinfo->index_->InsertEntry(tuple->KeyFromTuple(tableinfo_->schema_, *(indexinfo->index_->GetKeySchema()),
                                                         indexinfo->index_->GetKeyAttrs()),
                                     new_rid, exec_ctx_->GetTransaction());
      txn->GetIndexWriteSet()->emplace_back(
          IndexWriteRecord(new_rid, tableinfo_->oid_, WType::INSERT, *tuple, indexinfo->index_oid_, catalog_));
    }
  }

  if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ && lock_manager != nullptr) {
    lock_manager->Unlock(txn, new_rid);
  }
}
}  // namespace bustub
