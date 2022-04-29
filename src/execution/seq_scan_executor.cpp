//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
: AbstractExecutor(exec_ctx),plan_(plan),table_heap_(nullptr),iter_(nullptr,RID(),nullptr) {}

void SeqScanExecutor::Init() {
    table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
    iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
    if(iter_ == table_heap_->End()) {
        return false;
    }
    RID origin_rid = iter_->GetRid();
    const Schema* out_schema = plan_->OutputSchema();
    std::vector<Value> ans;
    int out_column_count = out_schema->GetColumnCount();
    for(int i = 0; i < out_column_count; i++) {
        ans.push_back(out_schema->GetColumn(i).GetExpr()->Evaluate(
            &(*iter_),&(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_))
        );
    }

    ++iter_;
    
    Tuple temp_tuple(ans,out_schema);
    const AbstractExpression* predicate = plan_->GetPredicate();
    if(!predicate || predicate->Evaluate(&temp_tuple,out_schema).GetAs<bool>()) {
        *tuple = temp_tuple;
        *rid = origin_rid;
        return true;
    }
    return Next(tuple,rid);
}

}  // namespace bustub
