#pragma once

#include <memory>
#include <string>

namespace opossum {

class AbstractOperator;
class Product;
class JoinHash;
class UnionPositions;
class TableScan;

class AbstractCostModelSampler {
 public:
  virtual ~AbstractCostModelSampler() = default;

  void sample(const std::shared_ptr<AbstractOperator>& pqp);

  virtual void write_samples() const = 0;

 protected:
  virtual void _sample_table_scan(const TableScan& table_scan) {}
  virtual void _sample_join_hash(const JoinHash& join_hash) {}
  virtual void _sample_product(const Product& join_hash) {}
  virtual void _sample_union_positions(const UnionPositions& union_positions) {}
};

}  // namespace opossum
