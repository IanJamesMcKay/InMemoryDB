#pragma once

#include <memory>
#include <unordered_map>

namespace opossum {

class TableStatistics;
class AbstractLQPNode;

struct LQPHash final {
  size_t operator()(const std::shared_ptr<AbstractLQPNode>& lqp) const;
};

struct LQPEqual final {
  size_t operator()(const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs) const;
};

class TableStatisticsCache final {
 public:
  std::shared_ptr<TableStatistics> get(const std::shared_ptr<AbstractLQPNode>& lqp) const;
  void put(const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<TableStatistics>& statistics);

  size_t hit_count() const;
  size_t miss_count() const;

 private:

  std::unordered_map<std::shared_ptr<AbstractLQPNode>,
                     std::shared_ptr<TableStatistics>,
                     LQPHash,
                     LQPEqual> _cache;
  mutable size_t _hit_count{0};
  mutable size_t _miss_count{0};
};
}  // namespace opossum