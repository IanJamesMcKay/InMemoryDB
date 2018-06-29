#pragma once

#include <memory>

#include "joe_config.hpp"
#include "joe_query.hpp"

namespace opossum {

class Joe final {
 public:
  Joe(const std::shared_ptr<JoeConfig>& config, const size_t workload_iteration_idx);

  void run();

  std::string prefix() const;

  std::shared_ptr<JoeConfig> config;
  const size_t workload_iteration_idx;

  std::vector<std::shared_ptr<JoeQuery>> queries;
};

}  // namespace opossum
