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
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(move(child_executor)) {}

void UpdateExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  std::vector<std::pair<Tuple,RID>> update_tuples;
  try {
    Tuple temp_tuple;
    RID temp_rid;
    while (child_executor_->Next(&temp_tuple, &temp_rid)) {
      update_tuples.push_back(std::pair<Tuple,RID>(temp_tuple,temp_rid));
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "Insert child execute error.");
    return false;
  }
  for(auto &tuple_t: update_tuples) {
    UpdateWithIndex(tuple_t.first,tuple_t.second);
  }
  return false;
}

void UpdateExecutor::UpdateWithIndex(Tuple &tuple,RID &rid) {
  Tuple new_tuple = GenerateUpdatedTuple(tuple);
  bool ok = table_heap_->UpdateTuple(new_tuple,rid,exec_ctx_->GetTransaction());
  if(ok) {
    for(auto &indexinfo: catalog_->GetTableIndexes(table_info_->name_)) {
      indexinfo->index_->DeleteEntry(tuple.KeyFromTuple(table_info_->schema_,
      *(indexinfo->index_->GetKeySchema()),indexinfo->index_->GetKeyAttrs()),rid,exec_ctx_->GetTransaction());
      indexinfo->index_->InsertEntry(new_tuple.KeyFromTuple(table_info_->schema_,
      *(indexinfo->index_->GetKeySchema()), indexinfo->index_->GetKeyAttrs()), rid, exec_ctx_->GetTransaction());
    } 
  }
}

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
