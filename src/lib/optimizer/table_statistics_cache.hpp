#pragma once

#include <memory>
#include <unordered_map>

namespace opossum {

class TableStatistics;
class AbstractLQPNode;

class TableStatisticsCache final {
 public:
  std::shared_ptr<TableStatistics> get(const std::shared_ptr<AbstractLQPNode>& lqp) const;
  void put(const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<TableStatistics>& statistics);

 private:
  struct LQPHash final {
    size_t operator()(const std::shared_ptr<AbstractLQPNode>& lqp) const;
  };

  struct LQPEqual final {
    size_t operator()(const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs) const;
  };

  std::unordered_map<std::shared_ptr<AbstractLQPNode>,
                     std::shared_ptr<TableStatistics>,
                     LQPHash,
                     LQPEqual> _cache;
};

}  // namespace opossum