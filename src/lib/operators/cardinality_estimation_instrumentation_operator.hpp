#pragma once

#include "abstract_read_only_operator.hpp"

namespace opossum {

class CardinalityEstimationCache;

class CardinalityEstimationInstrumentationOperator : public AbstractReadOnlyOperator {
 public:
  CardinalityEstimationInstrumentationOperator(const std::shared_ptr<const AbstractOperator>& left,
                                               const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache);

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  std::shared_ptr<CardinalityEstimationCache> _cardinality_estimation_cache;
};

}