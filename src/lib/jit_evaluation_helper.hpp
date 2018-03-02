#include <json.hpp>

#include <iostream>

namespace opossum {

// singleton
class JitEvaluationHelper {
 public:
  static JitEvaluationHelper& get();

  nlohmann::json& experiment() { return _experiment; }
  nlohmann::json& globals() { return _globals; }
  nlohmann::json& queries() { return _queries; }
  nlohmann::json& result() { return _result; }

 private:
  nlohmann::json _experiment;
  nlohmann::json _globals;
  nlohmann::json _queries;
  nlohmann::json _result;
};

}  // namespace opossum
