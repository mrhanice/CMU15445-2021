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
    : AbstractExecutor(exec_ctx),plan_(plan),table_heap_(nullptr),child_executor_(move(child_executor)) {}

void InsertExecutor::Init() {
    if(plan_->IsRawInsert()) {
        table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
        st_ = 0;    
    } else {
        table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
        child_executor_->Init();
    }
    catalog_ = exec_ctx_->GetCatalog();
    tableinfo_ = catalog_->GetTable(plan_->TableOid());
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    if(plan_->IsRawInsert()) {
        if(st_ == plan_->RawValues().size()) {
            return false;
        }
        const std::vector<Value> &row = plan_->RawValuesAt(st_);
        Schema schema = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->schema_;
        st_++;
        Tuple temp_tuple(row,&schema);
        RID new_rid;
        bool okinsert = table_heap_->InsertTuple(temp_tuple,&new_rid,exec_ctx_->GetTransaction());
        for(auto &indexinfo: catalog_->GetTableIndexes(tableinfo_->name_)) {
            indexinfo->index_->InsertEntry(temp_tuple.KeyFromTuple(tableinfo_->schema_, *(indexinfo->index_->GetKeySchema()), indexinfo->index_->GetKeyAttrs()), new_rid, exec_ctx_->GetTransaction());
        }
        return okinsert;
    } else {
        try {
            Tuple temp_tuple;
            RID temp_rid;
            bool ok = child_executor_->Next(&temp_tuple,&temp_rid);
            if(ok) {
                RID new_rid;
                bool okinsert = table_heap_->InsertTuple(temp_tuple,&new_rid,exec_ctx_->GetTransaction());
                for(auto &indexinfo: catalog_->GetTableIndexes(tableinfo_->name_)) {
                    indexinfo->index_->InsertEntry(temp_tuple.KeyFromTuple(tableinfo_->schema_, *(indexinfo->index_->GetKeySchema()), indexinfo->index_->GetKeyAttrs()), new_rid, exec_ctx_->GetTransaction());
                }
                return okinsert;
            } 
            return false;
        } catch (Exception &e) {
            throw Exception(ExceptionType::UNKNOWN_TYPE, "Insert child execute error.");
            return false;
        }
    }   
 }

}  // namespace bustub
