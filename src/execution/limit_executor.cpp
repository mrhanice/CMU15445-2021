//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(move(child_executor)), st_(0) {}

void LimitExecutor::Init() { child_executor_->Init(); }

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (st_ >= plan_->GetLimit()) {
    return false;
  }
  Tuple temp_tuple;
  RID temp_rid;
  bool ok = child_executor_->Next(&temp_tuple, &temp_rid);
  if (ok) {
    *tuple = temp_tuple;
    *rid = temp_rid;
    ++st_;
    return true;
  }
  return false;
}

}  // namespace bustub
