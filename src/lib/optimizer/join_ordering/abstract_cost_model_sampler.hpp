#pragma once

#include <memory>
#include <string>

namespace opossum {

class AbstractOperator;
class TableScan;

class AbstractCostModelSampler {
 public:
  virtual ~AbstractCostModelSampler() = default;

  void sample(const std::shared_ptr<AbstractOperator>& pqp);

  virtual void write_samples() const = 0;

 protected:
  virtual void _sample_table_scan(const TableScan& table_scan) {}
};

}  // namespace opossum
