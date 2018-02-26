#pragma once

#include <vector>

#include "boost/variant.hpp"

namespace opossum {

struct VizRecordLayout {
  std::vector<boost::variant<std::string, VizRecordLayout>> content;

  VizRecordLayout& add_label(const std::string& label);
  VizRecordLayout& add_sublayout();

  std::string to_label_string() const;
};

}  // namespace opossum
