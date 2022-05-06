//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    DistinctKey dis_key;
    size_t len = plan_->OutputSchema()->GetColumnCount();
    dis_key.distinct_key_.reserve(len);
    for (size_t i = 0; i < len; i++) {
      dis_key.distinct_key_.push_back(tuple.GetValue(plan_->OutputSchema(), i));
    }
    if (map_.count(dis_key) == 0) {
      map_.insert({dis_key, 1});
    }
  }
  iter_ = map_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == map_.end()) {
    return false;
  }
  const DistinctKey &dist = iter_->first;
  ++iter_;
  *tuple = Tuple(dist.distinct_key_, plan_->OutputSchema());
  return true;
}
}  // namespace bustub
