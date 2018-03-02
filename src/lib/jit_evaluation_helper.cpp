#include "jit_evaluation_helper.hpp"

namespace opossum {

// singleton
JitEvaluationHelper& JitEvaluationHelper::get() {
  static JitEvaluationHelper instance;
  return instance;
}

}  // namespace opossum
