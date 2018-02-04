#pragma once

#include <memory>

namespace opossum {

class Table;
class TableStatistics;

class TableStatisticsGenerator final {
 public:
  explicit TableStatisticsGenerator(const std::shared_ptr<const Table>& table);

  TableStatistics operator()();

 private:
  std::shared_ptr<const Table> _table;
};

}  // namespace opossum
