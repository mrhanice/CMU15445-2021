//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      st_(0) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  Tuple left_tuple;
  RID left_rid;
  // 对左表构建哈希表
  while (left_child_->Next(&left_tuple, &left_rid)) {
    HashKey hashkey;
    hashkey.column_key_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_child_->GetOutputSchema());
    if (map_.count(hashkey) != 0) {
      map_[hashkey].emplace_back(left_tuple);
    } else {
      map_[hashkey] = std::vector{left_tuple};
    }
  }
  Tuple right_tuple;
  RID right_rid;
  // hash Join
  while (right_child_->Next(&right_tuple, &right_rid)) {
    HashKey hashkey;
    hashkey.column_key_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_child_->GetOutputSchema());
    if (map_.count(hashkey) != 0) {
      std::vector<Tuple> &temp = map_[hashkey];
      for (auto &left_tuple_temp : temp) {
        std::vector<Value> output;
        for (const auto &col : plan_->OutputSchema()->GetColumns()) {
          output.push_back(col.GetExpr()->EvaluateJoin(&left_tuple_temp, left_child_->GetOutputSchema(), &right_tuple,
                                                       right_child_->GetOutputSchema()));
        }
        result_.emplace_back(Tuple(output, plan_->OutputSchema()));
      }
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (st_ == result_.size()) {
    return false;
  }
  *tuple = result_[st_];
  *rid = result_[st_].GetRid();
  ++st_;
  return true;
}

}  // namespace bustub
