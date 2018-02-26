#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "boost/dynamic_bitset.hpp"

namespace opossum {

class EnumerateCcp final {
 public:
  EnumerateCcp(size_t num_vertices, std::vector<std::pair<size_t, size_t>> edges);

  std::vector<std::pair<boost::dynamic_bitset<>, boost::dynamic_bitset<>>> operator()();

 private:
  size_t _num_vertices;
  std::vector<std::pair<size_t, size_t>> _edges;
  std::vector<std::pair<boost::dynamic_bitset<>, boost::dynamic_bitset<>>> _csg_cmp_pairs;

  void _enumerate_csg_recursive(std::vector<boost::dynamic_bitset<>>& csgs, const boost::dynamic_bitset<>& vertex_set,
                                const boost::dynamic_bitset<>& exclusion_set);
  void _enumerate_cmp(const boost::dynamic_bitset<>& vertex_set);
  boost::dynamic_bitset<> _exclusion_set(const size_t vertex_idx) const;
  boost::dynamic_bitset<> _neighbourhood(const boost::dynamic_bitset<>& vertex_set,
                                         const boost::dynamic_bitset<>& exclusion_set) const;
  std::vector<boost::dynamic_bitset<>> _non_empty_subsets(const boost::dynamic_bitset<>& vertex_set) const;
};

}  // namespace opossum
