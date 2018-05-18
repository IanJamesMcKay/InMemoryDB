#pragma once

#include <string>
#include <iostream>

namespace opossum {

template<typename T>
void write_csv(const std::vector<T>& data, const std::string& header, const std::string& path) {
  auto csv = std::ofstream{path};
  csv << header << "\n";
  for (const auto& row : data) {
    csv << row.sample << "\n";
  }
}

}  // namespace opossum