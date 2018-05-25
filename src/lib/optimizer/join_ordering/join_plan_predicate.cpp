#include "join_plan_predicate.hpp"

#include <boost/bimap.hpp>
#include "boost/functional/hash.hpp"
#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace {

using namespace opossum;  // NOLINT

/*
 * boost::bimap does not support initializer_lists.
 * Instead we use this helper function to have an initializer_list-friendly interface.
 */
template <typename L, typename R>
boost::bimap<L, R> make_bimap(std::initializer_list<typename boost::bimap<L, R>::value_type> list) {
  return boost::bimap<L, R>(list.begin(), list.end());
}

const boost::bimap<JoinPlanPredicateLogicalOperator, std::string> logical_operator_to_string =
make_bimap<JoinPlanPredicateLogicalOperator, std::string>({
                                            {JoinPlanPredicateLogicalOperator::And, "AND"},
                                            {JoinPlanPredicateLogicalOperator::Or, "OR"}});

// Among the vertices, find the index of the one that contains column_reference
size_t get_vertex_idx(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                      const LQPColumnReference& column_reference) {
  for (size_t vertex_idx = 0; vertex_idx < vertices.size(); ++vertex_idx) {
    if (vertices[vertex_idx]->find_output_column_id(column_reference)) return vertex_idx;
  }
  Fail("Couldn't find column " + column_reference.description() + " among vertices");
  return 0;
}
}  // namespace

namespace opossum {

AbstractJoinPlanPredicate::AbstractJoinPlanPredicate(const JoinPlanPredicateType type) : _type(type) {}

JoinPlanPredicateType AbstractJoinPlanPredicate::type() const { return _type; }

JoinPlanLogicalPredicate::JoinPlanLogicalPredicate(
    const std::shared_ptr<const AbstractJoinPlanPredicate>& left_operand,
    JoinPlanPredicateLogicalOperator logical_operator,
    const std::shared_ptr<const AbstractJoinPlanPredicate>& right_operand)
    : AbstractJoinPlanPredicate(JoinPlanPredicateType::LogicalOperator),
      left_operand(left_operand),
      logical_operator(logical_operator),
      right_operand(right_operand) {}

JoinVertexSet JoinPlanLogicalPredicate::get_accessed_vertex_set(
    const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices) const {
  return left_operand->get_accessed_vertex_set(vertices) | right_operand->get_accessed_vertex_set(vertices);
}

void JoinPlanLogicalPredicate::print(std::ostream& stream, const bool enclosing_braces) const {
  if (enclosing_braces) stream << "(";
  left_operand->print(stream, true);
  switch (logical_operator) {
    case JoinPlanPredicateLogicalOperator::And:
      stream << " AND ";
      break;
    case JoinPlanPredicateLogicalOperator::Or:
      stream << " OR ";
      break;
  }

  right_operand->print(stream, true);
  if (enclosing_braces) stream << ")";
}

nlohmann::json JoinPlanLogicalPredicate::to_json() const {
  nlohmann::json json;
  json["predicate_type"] = "logical";
  json["left_operand"] = left_operand->to_json();
  json["logical_operator"] = logical_operator_to_string.left.at(logical_operator);
  json["right_operand"] = right_operand->to_json();
  return json;
}

std::shared_ptr<const JoinPlanLogicalPredicate> JoinPlanLogicalPredicate::from_json(const nlohmann::json& json, std::vector<std::shared_ptr<AbstractLQPNode>>& vertices) {
  return std::make_shared<const JoinPlanLogicalPredicate>(
    join_plan_predicate_from_json(json["left_operand"], vertices),
    logical_operator_to_string.right.at(json["logical_operator"].get<std::string>()),
    join_plan_predicate_from_json(json["right_operand"], vertices)
  );
}

size_t JoinPlanLogicalPredicate::hash() const {
  auto hash = boost::hash_value(static_cast<size_t>(type()));
  boost::hash_combine(hash, left_operand->hash());
  boost::hash_combine(hash, static_cast<size_t>(logical_operator));
  boost::hash_combine(hash, right_operand->hash());
  return hash;
}

bool JoinPlanLogicalPredicate::operator==(const JoinPlanLogicalPredicate& rhs) const {
  return left_operand == rhs.left_operand && logical_operator == rhs.logical_operator &&
         right_operand == rhs.right_operand;
}

JoinPlanAtomicPredicate::JoinPlanAtomicPredicate(const LQPColumnReference& left_operand,
                                                 const PredicateCondition predicate_condition,
                                                 const AllParameterVariant& right_operand)
    : AbstractJoinPlanPredicate(JoinPlanPredicateType::Atomic),
      left_operand(left_operand),
      predicate_condition(predicate_condition),
      right_operand(right_operand) {
  DebugAssert(predicate_condition != PredicateCondition::Between,
              "Between not supported in JoinPlanPredicate, should be split up into two predicates");
}

JoinVertexSet JoinPlanAtomicPredicate::get_accessed_vertex_set(
    const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices) const {
  JoinVertexSet vertex_set{vertices.size()};
  vertex_set.set(get_vertex_idx(vertices, left_operand));
  if (is_lqp_column_reference(right_operand)) {
    vertex_set.set(get_vertex_idx(vertices, boost::get<LQPColumnReference>(right_operand)));
  }
  return vertex_set;
}

void JoinPlanAtomicPredicate::print(std::ostream& stream, const bool enclosing_braces) const {
  stream << left_operand.description() << " ";
  stream << predicate_condition_to_string.left.at(predicate_condition) << " ";
  stream << right_operand;
}

size_t JoinPlanAtomicPredicate::hash() const {
  auto hash = boost::hash_value(static_cast<size_t>(type()));
  boost::hash_combine(hash, std::hash<LQPColumnReference>{}(left_operand));
  boost::hash_combine(hash, static_cast<size_t>(predicate_condition));
  boost::hash_combine(hash, std::hash<AllParameterVariant>{}(right_operand));
  return hash;
}

nlohmann::json JoinPlanAtomicPredicate::to_json() const {
  auto json = nlohmann::json();
  json["predicate_type"] = "atomic";
  json["left_operand"] = left_operand.to_json();
  json["predicate_condition"] = predicate_condition_to_string.left.at(predicate_condition);

  if (is_lqp_column_reference(right_operand)) {
    json["right_operand_type"] = "column";
    json["right_operand"] = boost::get<LQPColumnReference>(right_operand).to_json();
  } else {
    json["right_operand_type"] = "value";
    json["right_operand"] = all_type_variant_to_json(boost::get<AllTypeVariant>(right_operand));
  }

  return json;
}

std::shared_ptr<const JoinPlanAtomicPredicate> JoinPlanAtomicPredicate::from_json(const nlohmann::json& json, std::vector<std::shared_ptr<AbstractLQPNode>>& vertices) {
  AllParameterVariant right_operand;

  if (json["right_operand_type"] == "column") {
    right_operand = LQPColumnReference::from_json(json["right_operand"], vertices);
  } else if (json["right_operand_type"] == "value") {
    right_operand = all_type_variant_from_json(json["right_operand"]);
  } else {
    Fail("Unknown right operand type");
  }

  return std::make_shared<const JoinPlanAtomicPredicate>(
    LQPColumnReference::from_json(json["left_operand"], vertices),
    predicate_condition_to_string.right.at(json["predicate_condition"].get<std::string>()),
    right_operand
  );
}

bool JoinPlanAtomicPredicate::operator==(const JoinPlanAtomicPredicate& rhs) const {
  return left_operand == rhs.left_operand && predicate_condition == rhs.predicate_condition &&
         right_operand == rhs.right_operand;
}

std::shared_ptr<const AbstractJoinPlanPredicate> join_plan_predicate_from_json(const nlohmann::json& json,
                                                                               std::vector<std::shared_ptr<AbstractLQPNode>>& vertices) {
  if (json["predicate_type"] == "atomic") {
    return JoinPlanAtomicPredicate::from_json(json, vertices);
  } else if (json["predicate_type"] == "logical") {
    return JoinPlanLogicalPredicate::from_json(json, vertices);
  } else {
    Fail("Unknown predicate type");
  }


}

}  // namespace opossum
