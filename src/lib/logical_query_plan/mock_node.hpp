#pragma once

#include <memory>
#include <optional>
#include <string>
#include "boost/variant.hpp"

#include "abstract_lqp_node.hpp"
#include "lqp_column_reference.hpp"
#include "all_type_variant.hpp"

namespace opossum {

class TableStatistics;

/**
 * Node that represents a table that has no data backing it, but may provide
 *  - (mocked) statistics
 *  - or just a column layout. It will pretend it created the columns.
 * It is useful in tests (e.g. general LQP tests, optimizer tests that just rely on statistics and not actual data) and
 * the playground
 */
class MockNode : public EnableMakeForLQPNode<MockNode>, public AbstractLQPNode {
 public:
  using ColumnDefinitions = std::vector<std::pair<DataType, std::string>>;

  explicit MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& name = {});
  explicit MockNode(const std::shared_ptr<TableStatistics>& statistics);

  LQPColumnReference get_column(const std::string& name) const;

  const ColumnDefinitions& column_definitions() const;
  const boost::variant<ColumnDefinitions, std::shared_ptr<TableStatistics>>& constructor_arguments() const;

  const std::vector<std::shared_ptr<AbstractExpression>>& output_column_expressions() const override;

  std::string description(const DescriptionMode description_mode = DescriptionMode::SingleLine) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _shallow_copy_impl(LQPNodeMapping & node_mapping) const override;
  bool _shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const override;

 private:
  std::optional<std::string> _name;
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> _output_column_expressions;

  // Constructor args to keep around for deep_copy()
  boost::variant<ColumnDefinitions, std::shared_ptr<TableStatistics>> _constructor_arguments;
};
}  // namespace opossum
