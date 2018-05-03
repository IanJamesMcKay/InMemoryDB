#include "join_edge.hpp"

#include "join_plan_predicate.hpp"

namespace opossum {

JoinEdge::JoinEdge(const JoinVertexSet& vertex_set, const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>&predicates) : vertex_set(vertex_set), predicates(predicates) {}

void JoinEdge::print(std::ostream& stream) const {
  stream << "Edge between " << vertex_set << ", " << predicates.size() << " predicates" << std::endl;
  for (const auto& predicate : predicates) {
    predicate->print(stream);
    stream << std::endl;
  }
}
}  // namespace opossum
