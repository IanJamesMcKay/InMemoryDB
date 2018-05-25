#include "lqp_column_reference.hpp"

#include "boost/functional/hash.hpp"

#include "abstract_lqp_node.hpp"
#include "stored_table_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

LQPColumnReference::LQPColumnReference(const std::shared_ptr<const AbstractLQPNode>& original_node,
                                       ColumnID original_column_id)
    : _original_node(original_node), _original_column_id(original_column_id) {}

std::shared_ptr<const AbstractLQPNode> LQPColumnReference::original_node() const { return _original_node.lock(); }

ColumnID LQPColumnReference::original_column_id() const { return _original_column_id; }

std::string LQPColumnReference::description() const {
  const auto node = this->original_node();

  DebugAssert(node, "LQPColumnReference state not sufficient to retrieve column name");
  return node->get_verbose_column_name(_original_column_id);
}

nlohmann::json LQPColumnReference::to_json() const {
  Assert(original_node()->type() == LQPNodeType::StoredTable, "to_json() only supports references to StoredTableNode");

  auto json = nlohmann::json();

  const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(original_node());
  json["original_node_table_name"] = stored_table_node->table_name();
  if (stored_table_node->alias()) {
    json["original_node_alias"] = *stored_table_node->alias();
  }

  json["original_column_id"] = static_cast<size_t>(_original_column_id.t);

  return json;
}

LQPColumnReference LQPColumnReference::from_json(const nlohmann::json& json,
                                                 std::vector<std::shared_ptr<AbstractLQPNode>>& vertices) {
  const auto table_name = json["original_node_table_name"].get<std::string>();
  const auto alias = json.count("original_node_alias") ? std::optional<std::string>{json["original_node_alias"].get<std::string>()} : std::nullopt;

  for (const auto& vertex : vertices) {
    Assert(vertex->type() == LQPNodeType::StoredTable, "Only StoredTableNodes supported right now");
    const auto stored_table_node = std::static_pointer_cast<StoredTableNode>(vertex);

    if (table_name == stored_table_node->table_name() && alias == stored_table_node->alias()) {
      return stored_table_node->output_column_references().at(json["original_column_id"].get<size_t>());
    }
  }

  Fail("Couldn't resolve column");
}

bool LQPColumnReference::operator==(const LQPColumnReference& rhs) const {
  return original_node() == rhs.original_node() && _original_column_id == rhs._original_column_id;
}

std::ostream& operator<<(std::ostream& os, const LQPColumnReference& column_reference) {
  const auto node = column_reference.original_node();

  if (column_reference.original_node()) {
    os << column_reference.description();
  } else {
    os << "[Invalid LQPColumnReference, ColumnID:" << column_reference.original_column_id() << "]";
  }
  return os;
}
}  // namespace opossum

namespace std {

size_t hash<opossum::LQPColumnReference>::operator()(const opossum::LQPColumnReference& lqp_column_reference) const {
  const auto original_node = lqp_column_reference.original_node();
  DebugAssert(original_node, "original_node needs to exist for hashing to work");

  auto hash = original_node->hash();
  boost::hash_combine(hash, lqp_column_reference.original_column_id().t);
  return hash;
}

} // namespace std