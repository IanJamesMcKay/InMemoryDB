#include "jit_evaluation_helper.hpp"

namespace opossum {

int32_t JitEvaluationHelper::papi_events = { PAPI_TOT_CYC, PAPI_TOT_INS };

std::string JitEvaluationHelper::papi_names = { "PAPI_TOT_CYC", "PAPI_TOT_INS" };

// singleton
JitEvaluationHelper& JitEvaluationHelper::get() {
  static JitEvaluationHelper instance;
  return instance;
}

}  // namespace opossum
