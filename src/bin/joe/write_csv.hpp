#pragma once

#include <string>
#include <iostream>

namespace opossum {

template<typename T>
auto get_sample(const T& t) -> decltype(T::sample) {
  return t.sample;
}

template<typename T>
auto get_sample(const std::shared_ptr<T>& t) -> decltype(T::sample) {
  return t->sample;
}

template<typename T>
void write_csv(const std::vector<T>& data, const std::string& header, const std::string& path) {
  auto csv = std::ofstream{path};
  csv << header << "\n";
  for (const auto& row : data) {
    csv << get_sample(row) << "\n";
  }
}

}  // namespace opossum