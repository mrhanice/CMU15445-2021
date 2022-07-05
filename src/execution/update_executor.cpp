//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(move(child_executor)) {}

void UpdateExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  std::vector<std::pair<Tuple, RID>> update_tuples;
  try {
    Tuple temp_tuple;
    RID temp_rid;
    while (child_executor_->Next(&temp_tuple, &temp_rid)) {
      update_tuples.emplace_back(std::pair<Tuple, RID>(temp_tuple, temp_rid));
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "Insert child execute error.");
    return false;
  }

  LockManager *lock_manager = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();

  for (auto &tuple_t : update_tuples) {
    // UpdateWithIndex(tuple_t.first, tuple_t.second);

    // 加锁
    if (lock_manager != nullptr) {
      if (txn->IsSharedLocked(tuple_t.second)) {
        lock_manager->LockUpgrade(txn, tuple_t.second);
      } else {
        lock_manager->LockExclusive(txn, tuple_t.second);
      }
    }

    Tuple new_tuple = GenerateUpdatedTuple(tuple_t.first);
    bool ok = table_heap_->UpdateTuple(new_tuple, tuple_t.second, exec_ctx_->GetTransaction());

    if (ok) {
      for (auto &indexinfo : catalog_->GetTableIndexes(table_info_->name_)) {
        // 不要求索引并发处理，只更新索引写集就可
        indexinfo->index_->DeleteEntry(
            tuple_t.first.KeyFromTuple(table_info_->schema_, *(indexinfo->index_->GetKeySchema()),
                                       indexinfo->index_->GetKeyAttrs()),
            tuple_t.second, exec_ctx_->GetTransaction());
        indexinfo->index_->InsertEntry(
            new_tuple.KeyFromTuple(table_info_->schema_, *(indexinfo->index_->GetKeySchema()),
                                   indexinfo->index_->GetKeyAttrs()),
            tuple_t.second, exec_ctx_->GetTransaction());
        // 添加索引写集
        IndexWriteRecord index_write_record(tuple_t.second, table_info_->oid_, WType::UPDATE, new_tuple,
                                            indexinfo->index_oid_, catalog_);
        index_write_record.old_tuple_ = tuple_t.first;
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

// void UpdateExecutor::UpdateWithIndex(Tuple &tuple, RID &rid) {
//   Tuple new_tuple = GenerateUpdatedTuple(tuple);
//   bool ok = table_heap_->UpdateTuple(new_tuple, rid, exec_ctx_->GetTransaction());
//   if (ok) {
//     for (auto &indexinfo : catalog_->GetTableIndexes(table_info_->name_)) {
//       indexinfo->index_->DeleteEntry(tuple.KeyFromTuple(table_info_->schema_, *(indexinfo->index_->GetKeySchema()),
//                                                         indexinfo->index_->GetKeyAttrs()),
//                                      rid, exec_ctx_->GetTransaction());
//       indexinfo->index_->InsertEntry(new_tuple.KeyFromTuple(table_info_->schema_,
//       *(indexinfo->index_->GetKeySchema()),
//                                                             indexinfo->index_->GetKeyAttrs()),
//                                      rid, exec_ctx_->GetTransaction());
//     }
//   }
// }

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
