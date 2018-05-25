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

nlohmann::json all_type_variant_to_json(const AllTypeVariant& value) {
  nlohmann::json json;
  if (value.type() == typeid(int32_t)) {
    json["type"] = "int";
    json["value"] = boost::get<int32_t>(value);
  } else if (value.type() == typeid(int64_t)) {
    json["type"] = "long";
    json["value"] = boost::get<int64_t>(value);
  } else if (value.type() == typeid(float)) {
    json["type"] = "float";
    json["value"] = boost::get<float>(value);
  } else if (value.type() == typeid(double)) {
    json["type"] = "double";
    json["value"] = boost::get<double>(value);
  } else if (value.type() == typeid(std::string)) {
    json["type"] = "string";
    json["value"] = boost::get<std::string>(value);
  } else if (value.type() == typeid(NullValue)) {
    json["type"] = "null";
  } else {
    Fail("unexpected type");
  }
  return json;
}

AllTypeVariant all_type_variant_from_json(const nlohmann::json& json) {
  const auto type = json["type"].get<std::string>();

  if (type == "int") {
    return json["value"].get<int32_t>();
  } else if (type == "long") {
    return json["value"].get<int64_t>();
  } else if (type == "float") {
    return json["value"].get<float>();
  } else if (type == "double") {
    return json["value"].get<double>();
  } else if (type == "string") {
    return json["value"].get<std::string>();
  } else if (type == "null") {
    return NullValue{};
  } else {
    Fail("Unexpected type");
  }
}

}  // namespace opossum


namespace std {

size_t hash<opossum::AllTypeVariant>::operator()(const opossum::AllTypeVariant& all_type_variant) const {
  auto hash = all_type_variant.type().hash_code();
  boost::hash_combine(hash, boost::apply_visitor([](const auto& value) { return std::hash<std::decay_t<decltype(value)>>{}(value); }, all_type_variant));
  return hash;
}

}  // namespace std
