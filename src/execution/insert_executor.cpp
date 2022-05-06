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

void InsertExecutor::InsertIntoTableWithIndex(Tuple *tuple) {
  RID new_rid;
  bool okinsert = table_heap_->InsertTuple(*tuple, &new_rid, exec_ctx_->GetTransaction());
  if (okinsert) {
    for (auto &indexinfo : catalog_->GetTableIndexes(tableinfo_->name_)) {
      indexinfo->index_->InsertEntry(tuple->KeyFromTuple(tableinfo_->schema_, *(indexinfo->index_->GetKeySchema()),
                                                         indexinfo->index_->GetKeyAttrs()),
                                     new_rid, exec_ctx_->GetTransaction());
    }
  }
}
}  // namespace bustub
