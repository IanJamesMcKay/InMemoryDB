#include "cardinality_estimation_cache.hpp"

#include <algorithm>
#include <iostream>
#include <fstream>

#include "json.hpp"

#include "optimizer/join_ordering/join_plan_predicate.hpp"

namespace opossum {

std::optional<Cardinality> CardinalityEstimationCache::get(const BaseJoinGraph& join_graph) {
  auto normalized_join_graph = _normalize(join_graph);

  auto& entry = _cache[normalized_join_graph];

  if (_log) (*_log) << "CardinalityEstimationCache [" << (entry.request_count == 0 ? "I" : "S") << "]";

  ++entry.request_count;

  std::optional<Cardinality> result;

  if (entry.cardinality) {
    if (_log) (*_log) << "[HIT ]: ";
    ++_hit_count;
    result = *entry.cardinality;
  } else {
    if (_log) (*_log) << "[MISS]: ";
    ++_miss_count;
  }

  if (_log) (*_log) << normalized_join_graph.description();

  if (entry.cardinality) {
    if (_log) (*_log) << ": " << *entry.cardinality;
  }

  if (_log) (*_log) << std::endl;

  return result;
}

void CardinalityEstimationCache::put(const BaseJoinGraph& join_graph, const Cardinality cardinality) {
  auto normalized_join_graph = _normalize(join_graph);

  auto& entry = _cache[normalized_join_graph];

  if (_log && !entry.cardinality) {
    (*_log) << "CardinalityEstimationCache [" << (entry.request_count == 0 ? "I" : "S") << "][PUT ]: " << normalized_join_graph.description() << ": " << cardinality << std::endl;
  }

  entry.cardinality = cardinality;
}

size_t CardinalityEstimationCache::cache_hit_count() const {
  return _hit_count;
}

size_t CardinalityEstimationCache::cache_miss_count() const {
  return _miss_count;
}

size_t CardinalityEstimationCache::size() const {
  return _cache.size();
}

size_t CardinalityEstimationCache::distinct_request_count() const {
  return _cache.size();
}

size_t CardinalityEstimationCache::distinct_hit_count() const {
  return std::count_if(_cache.begin(), _cache.end(), [](const auto& entry) -> bool { return entry.second.cardinality && entry.second.request_count > 0; });
}

size_t CardinalityEstimationCache::distinct_miss_count() const {
  return std::count_if(_cache.begin(), _cache.end(), [](const auto& entry) -> bool { return !entry.second.cardinality && entry.second.request_count > 0; });
}

void CardinalityEstimationCache::reset_distinct_hit_miss_counts() {
  for (auto &entry : _cache) entry.second.request_count = 0;
}

void CardinalityEstimationCache::clear() {
  _cache.clear();
  _hit_count = 0;
  _miss_count = 0;
  _log.reset();
}

void CardinalityEstimationCache::set_log(const std::shared_ptr<std::ostream>& log) {
  _log = log;
}

void CardinalityEstimationCache::print(std::ostream& stream) const {
  stream << "-------------------- ENGAGED ENTRIES ------------------------" << std::endl;
  for (const auto& pair : _cache) {
    if (pair.second.cardinality) {
      stream << pair.first.description() << ": " << *pair.second.cardinality << std::endl;
    }
  }
  stream << std::endl;
  stream << "------------------- DISENGAGED ENTRIES ------------------------" << std::endl;
  for (const auto& pair : _cache) {
    if (!pair.second.cardinality) {
      stream << pair.first.description() << ": -" << std::endl;
    }
  }
}

BaseJoinGraph CardinalityEstimationCache::_normalize(const BaseJoinGraph& join_graph) {
  auto normalized_join_graph = join_graph;

  for (auto& predicate : normalized_join_graph.predicates) {
    predicate = _normalize(predicate);
  }

  return normalized_join_graph;
}

std::shared_ptr<CardinalityEstimationCache> CardinalityEstimationCache::load(const std::string& path) {
  std::ifstream stream{path};
  Assert(stream.is_open(), "Couldn't open persistent CardinalityEstimationCache");

  nlohmann::json json;
  stream >> json;

  return from_json(json);
}

void CardinalityEstimationCache::store(const std::string& path) const {
  std::ofstream stream{path};
  stream << to_json();
}

nlohmann::json CardinalityEstimationCache::to_json() const {
  nlohmann::json json;

  json = nlohmann::json::array();

  for (const auto& pair : _cache) {
    if (!pair.second.cardinality) continue;

    auto entry_json = nlohmann::json();

    entry_json["key"] = pair.first.to_json();
    entry_json["value"] = *pair.second.cardinality;

    json.push_back(entry_json);
  }

  return json;
}

std::shared_ptr<CardinalityEstimationCache> CardinalityEstimationCache::from_json(const nlohmann::json& json) {
  const auto cache = std::make_shared<CardinalityEstimationCache>();

  for (const auto& pair : json) {
    const auto key = BaseJoinGraph::from_json(pair["key"]);
    const auto value = pair["value"].get<Cardinality>();
    cache->put(key, value);
  }

  return cache;
}

std::shared_ptr<const AbstractJoinPlanPredicate> CardinalityEstimationCache::_normalize(const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate) {
  if (predicate->type() == JoinPlanPredicateType::Atomic) {
    const auto atomic_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate);
    if (is_lqp_column_reference(atomic_predicate->right_operand)) {
      const auto right_operand_column_reference = boost::get<LQPColumnReference>(atomic_predicate->right_operand);

      if (std::hash<LQPColumnReference>{}(right_operand_column_reference) < std::hash<LQPColumnReference>{}(atomic_predicate->left_operand)) {
        if (atomic_predicate->predicate_condition != PredicateCondition::Like) {
          const auto flipped_predicate_condition = flip_predicate_condition(atomic_predicate->predicate_condition);
          return std::make_shared<JoinPlanAtomicPredicate>(right_operand_column_reference, flipped_predicate_condition, atomic_predicate->left_operand);
        }
      }
    }
  } else {
    const auto logical_predicate = std::static_pointer_cast<const JoinPlanLogicalPredicate>(predicate);

    auto normalized_left = _normalize(logical_predicate->left_operand);
    auto normalized_right = _normalize(logical_predicate->right_operand);

    if (normalized_right->hash() < normalized_left->hash()) {
      std::swap(normalized_left, normalized_right);
    }

    return std::make_shared<JoinPlanLogicalPredicate>(normalized_left,
                                                      logical_predicate->logical_operator,
                                                      normalized_right);

  }

  return predicate;
}

}  // namespace opossum