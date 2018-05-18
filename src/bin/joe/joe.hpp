#pragma once

#include <memory>

#include "joe_config.hpp"
//#include "joe_plan.hpp"
//#include "joe_query_iteration.hpp"
#include "joe_query.hpp"

namespace opossum {

class Joe final {
 public:
  explicit Joe(const std::shared_ptr<JoeConfig>& config);

  void run();

  std::shared_ptr<JoeConfig> config;

  std::vector<JoeQuery> queries;
};

}  // namespace opossum
