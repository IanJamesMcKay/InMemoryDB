#pragma once

#include <chrono>

namespace opossum {

struct BaseOperatorPerformanceData {
  virtual ~BaseOperatorPerformanceData() = default;

  std::chrono::nanoseconds total;
};

}  // namespace opossum
