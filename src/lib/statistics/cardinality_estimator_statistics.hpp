#pragma once

#include <unordered_map>
#include <set>

#include "abstract_cardinality_estimator.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"

namespace opossum {

class AbstractColumnStatistics;
class JoinNode;
class TableStatistics;
class PredicateNode;
class UnionNode;
class MockNode;
class StoredTableNode;

class CardinalityEstimatorStatistics : public AbstractCardinalityEstimator {
 public:
  Cardinality estimate(const std::shared_ptr<AbstractLQPNode>& lqp,
                       const std::shared_ptr<AbstractQueryBlock>& query_blocks_root = {}) const override;

 private:
  static std::shared_ptr<TableStatistics> _compute_node_statistics(const std::shared_ptr<AbstractLQPNode> &node);
  static std::shared_ptr<TableStatistics> _compute_join_node_statistics(const std::shared_ptr<JoinNode> &node);
  static std::shared_ptr<TableStatistics> _compute_predicate_node_statistics(const std::shared_ptr<PredicateNode> &node);
  static std::shared_ptr<TableStatistics> _compute_union_node_statistics(const std::shared_ptr<UnionNode> &node);
  static std::shared_ptr<TableStatistics> _compute_mock_node_statistics(const std::shared_ptr<MockNode> &node);
  static std::shared_ptr<TableStatistics> _compute_stored_table_node_statistics(const std::shared_ptr<StoredTableNode> &node);
};

}  // namespace opossum