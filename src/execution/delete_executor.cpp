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
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(move(child_executor)) {}

void DeleteExecutor::Init() {
    child_executor_->Init();
    catalog_ = exec_ctx_->GetCatalog();
    table_info_ = catalog_->GetTable(plan_->TableOid());
    table_heap_ = table_info_->table_.get();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    std::vector<std::pair<Tuple,RID>> delete_tuples;
    try {
        Tuple temp_tuple;
        RID temp_rid;
        while (child_executor_->Next(&temp_tuple, &temp_rid)) {
            delete_tuples.push_back(std::pair<Tuple,RID>(temp_tuple,temp_rid));
        }
    } catch (Exception &e) {
        throw Exception(ExceptionType::UNKNOWN_TYPE, "Insert child execute error.");
        return false;
    }
    for(auto &tuple_t: delete_tuples) {
        DeleteWithIndex(tuple_t.first,tuple_t.second);
    }
    return false;
}

void DeleteExecutor::DeleteWithIndex(Tuple &tuple, RID &rid) {
    bool ok = table_heap_->MarkDelete(rid,exec_ctx_->GetTransaction());
    if(ok) {
        for(auto &indexinfo: catalog_->GetTableIndexes(table_info_->name_)) {
        indexinfo->index_->DeleteEntry(tuple.KeyFromTuple(table_info_->schema_,
        *(indexinfo->index_->GetKeySchema()),indexinfo->index_->GetKeyAttrs()),rid,exec_ctx_->GetTransaction());
        } 
    }
}

}  // namespace bustub



