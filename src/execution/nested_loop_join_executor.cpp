//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(move(left_executor)),
      right_executor_(move(right_executor)),
      st_(0) {}

void NestedLoopJoinExecutor::Init() {
  Tuple left_tuple;
  Tuple right_tuple;
  RID left_rid;
  RID right_rid;
  left_executor_->Init();
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      const Schema *output_schema = plan_->OutputSchema();
      if (plan_->Predicate() == nullptr || plan_->Predicate()
                                               ->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                              &right_tuple, right_executor_->GetOutputSchema())
                                               .GetAs<bool>()) {
        std::vector<Value> temp_ans;
        for (auto &column : output_schema->GetColumns()) {
          Value temp_v = column.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                        right_executor_->GetOutputSchema());
          temp_ans.push_back(temp_v);
        }
        result_.emplace_back(Tuple(temp_ans, output_schema));
      }
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (st_ == result_.size()) {
    return false;
  }
  *tuple = result_[st_];
  *rid = result_[st_].GetRid();
  ++st_;
  return true;
}
}  // namespace bustub
