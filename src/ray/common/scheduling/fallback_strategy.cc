// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/scheduling/fallback_strategy.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/constants.h"
#include "ray/util/logging.h"

namespace ray {

void FallbackOption::ToProto(rpc::FallbackOption *proto) const {
  RAY_CHECK(proto != nullptr);
  label_selector.ToProto(proto->mutable_label_selector());
  // When a new option is added, add its serialization here.
}

std::shared_ptr<std::vector<FallbackOption>> ParseFallbackStrategy(
    const google::protobuf::RepeatedPtrField<rpc::FallbackOption> &strategy_proto_list) {
  auto strategy_list = std::make_shared<std::vector<FallbackOption>>();
  strategy_list->reserve(strategy_proto_list.size());

  for (const auto &strategy_proto : strategy_proto_list) {
    strategy_list->emplace_back(strategy_proto.label_selector());
  }

  return strategy_list;
}

rpc::FallbackStrategy SerializeFallbackStrategy(
    const std::vector<FallbackOption> &strategy_list) {
  rpc::FallbackStrategy strategy_proto;
  for (const auto &options : strategy_list) {
    options.ToProto(strategy_proto.add_options());
  }

  return strategy_proto;
}

namespace {

bool CanMatchNode(const LabelSelector &selector, const std::string &node_id) {
  for (const auto &constraint : selector.GetConstraints()) {
    if (constraint.GetLabelKey() != kLabelKeyNodeID) {
      continue;
    }
    const bool contains_node = constraint.GetLabelValues().contains(node_id);
    if ((constraint.GetOperator() == LabelSelectorOperator::LABEL_IN && !contains_node) ||
        (constraint.GetOperator() == LabelSelectorOperator::LABEL_NOT_IN &&
         contains_node)) {
      return false;
    }
  }
  return true;
}

LabelSelector ExcludingNode(LabelSelector selector, const std::string &node_id) {
  selector.AddConstraint(
      LabelConstraint(kLabelKeyNodeID, LabelSelectorOperator::LABEL_NOT_IN, {node_id}));
  return selector;
}

}  // namespace

void ApplySoftNodeExclusion(const std::string &excluded_node_id,
                            LabelSelector *primary,
                            std::vector<FallbackOption> *fallbacks) {
  RAY_CHECK(primary != nullptr);
  RAY_CHECK(fallbacks != nullptr);
  if (excluded_node_id.empty()) {
    return;
  }

  const LabelSelector original_primary = *primary;
  std::vector<FallbackOption> original_fallbacks = std::move(*fallbacks);
  fallbacks->clear();

  if (CanMatchNode(original_primary, excluded_node_id)) {
    *primary = ExcludingNode(original_primary, excluded_node_id);
    fallbacks->emplace_back(original_primary);
  }

  for (const auto &fallback : original_fallbacks) {
    if (CanMatchNode(fallback.label_selector, excluded_node_id)) {
      fallbacks->emplace_back(ExcludingNode(fallback.label_selector, excluded_node_id));
    }
    fallbacks->push_back(fallback);
  }
}

}  // namespace ray
