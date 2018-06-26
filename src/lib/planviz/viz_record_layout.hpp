#pragma once

#include <vector>

#include "boost/variant.hpp"

namespace opossum {

class VizRecordLayout {
 public:
  std::vector<boost::variant<std::string, VizRecordLayout>> content;

  VizRecordLayout& add_label(const std::string& label);
  VizRecordLayout& add_sublayout();

  std::string to_label_string() const;

 private:
  static std::string _escape(std::string string);
};

}  // namespace opossum
