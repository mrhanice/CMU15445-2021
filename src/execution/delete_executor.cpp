//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  std::vector<std::pair<Tuple, RID>> delete_tuples;
  try {
    Tuple temp_tuple;
    RID temp_rid;
    while (child_executor_->Next(&temp_tuple, &temp_rid)) {
      delete_tuples.emplace_back(std::pair<Tuple, RID>(temp_tuple, temp_rid));
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "Insert child execute error.");
    return false;
  }
  LockManager *lock_manager = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();

  for (auto &tuple_t : delete_tuples) {
    // DeleteWithIndex(tuple_t.first, tuple_t.second);
    // 加锁
    if (lock_manager != nullptr) {
      if (txn->IsSharedLocked(tuple_t.second)) {
        lock_manager->LockUpgrade(txn, tuple_t.second);
      } else {
        lock_manager->LockExclusive(txn, tuple_t.second);
      }
    }
    bool ok = table_heap_->MarkDelete(tuple_t.second, exec_ctx_->GetTransaction());

    if (ok) {
      for (auto &indexinfo : catalog_->GetTableIndexes(table_info_->name_)) {
        indexinfo->index_->DeleteEntry(
            tuple_t.first.KeyFromTuple(table_info_->schema_, *(indexinfo->index_->GetKeySchema()),
                                       indexinfo->index_->GetKeyAttrs()),
            tuple_t.second, exec_ctx_->GetTransaction());
        // 更新索引写集
        IndexWriteRecord index_write_record(tuple_t.second, table_info_->oid_, WType::DELETE, tuple_t.first,
                                            indexinfo->index_oid_, catalog_);
        txn->GetIndexWriteSet()->emplace_back(index_write_record);
      }
    }

    // 解锁
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ && lock_manager != nullptr) {
      lock_manager->Unlock(txn, tuple_t.second);
    }
  }
  return false;
}

// void DeleteExecutor::DeleteWithIndex(Tuple &tuple, RID &rid) {
//   bool ok = table_heap_->MarkDelete(rid, exec_ctx_->GetTransaction());
//   if (ok) {
//     for (auto &indexinfo : catalog_->GetTableIndexes(table_info_->name_)) {
//       indexinfo->index_->DeleteEntry(tuple.KeyFromTuple(table_info_->schema_, *(indexinfo->index_->GetKeySchema()),
//                                                         indexinfo->index_->GetKeyAttrs()),
//                                      rid, exec_ctx_->GetTransaction());
//     }
//   }
// }

}  // namespace bustub
