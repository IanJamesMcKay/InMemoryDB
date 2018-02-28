#include <json.hpp>

#include <iostream>

namespace opossum {

// singleton
class JitEvaluationHelper {
 public:
  static JitEvaluationHelper& get();

  nlohmann::json& experiment() { return _experiment; }
  nlohmann::json& queries() { return _queries; }

 private:
  nlohmann::json _experiment;
  nlohmann::json _queries;
};

}  // namespace opossum
