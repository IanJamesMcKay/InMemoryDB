#include "all_type_variant.hpp"

#include <cmath>

#include "boost/functional/hash.hpp"
#include "boost/lexical_cast.hpp"

namespace opossum {

bool all_type_variant_near(const AllTypeVariant& lhs, const AllTypeVariant& rhs, double max_abs_error) {
  // TODO(anybody) no checks for float vs double etc yet.

  if (lhs.type() == typeid(float) && rhs.type() == typeid(float)) {  //  NOLINT - lint thinks this is a C-style cast
    return std::fabs(boost::get<float>(lhs) - boost::get<float>(rhs)) < max_abs_error;
  } else if (lhs.type() == typeid(double) && rhs.type() == typeid(double)) {  //  NOLINT - see above
    return std::fabs(boost::get<double>(lhs) - boost::get<double>(rhs)) < max_abs_error;
  }

  return lhs == rhs;
}

}  // namespace opossum


namespace std {

size_t hash<opossum::AllTypeVariant>::operator()(const opossum::AllTypeVariant& all_type_variant) const {
  auto hash = all_type_variant.type().hash_code();
  boost::hash_combine(hash, boost::apply_visitor([](const auto& value) { return std::hash<std::decay_t<decltype(value)>>{}(value); }, all_type_variant));
  return hash;
}

}  // namespace std
