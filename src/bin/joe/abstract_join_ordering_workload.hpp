#pragma once

#include <string>

namespace opossum {

using QueryID = size_t;

class AbstractJoinOrderingWorkload {
 public:
  virtual ~AbstractJoinOrderingWorkload() = default;

  virtual void setup() = 0;
  virtual size_t query_count() const = 0;
  virtual std::string get_query(const size_t query_idx) const = 0;
  virtual std::string get_query_name(const size_t query_idx) const = 0;
};

}  // namespace opossum
