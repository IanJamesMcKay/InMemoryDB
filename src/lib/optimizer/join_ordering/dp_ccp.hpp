#pragma once

#include "abstract_dp_algorithm.hpp"
#include "dp_subplan_cache_best.hpp"

namespace opossum {

class DpCcp : public AbstractDpAlgorithm {
 public:
  DpCcp();

 protected:
  void _on_execute() override;
};

}  // namespace opossum
