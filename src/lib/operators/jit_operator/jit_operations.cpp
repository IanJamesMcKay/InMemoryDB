#include "jit_operations.hpp"

namespace opossum {

void jit_not(const JitMaterializedValue &lhs, JitMaterializedValue &result) {
  DebugAssert(lhs.data_type() == DataType::Bool && result.data_type() == DataType::Bool, "invalid type for operation");
  result.set<bool>(!lhs.get<bool>());
  result.set_is_null(lhs.is_null());
}

void jit_and(const JitMaterializedValue &lhs, const JitMaterializedValue &rhs, JitMaterializedValue &result) {
  DebugAssert(
          lhs.data_type() == DataType::Bool && rhs.data_type() == DataType::Bool &&
          result.data_type() == DataType::Bool,
          "invalid type for operation");

  // three-valued logic AND
  if (lhs.is_null()) {
    result.set<bool>(false);
    result.set_is_null(rhs.is_null() || rhs.get<bool>());
  } else {
    result.set<bool>(lhs.get<bool>() && rhs.get<bool>());
    result.set_is_null(lhs.get<bool>() && rhs.is_null());
  }
}

void jit_or(const JitMaterializedValue &lhs, const JitMaterializedValue &rhs, JitMaterializedValue &result) {
  DebugAssert(
          lhs.data_type() == DataType::Bool && rhs.data_type() == DataType::Bool &&
          result.data_type() == DataType::Bool,
          "invalid type for operation");

  // three-valued logic OR
  if (lhs.is_null()) {
    result.set<bool>(true);
    result.set_is_null(rhs.is_null() || !rhs.get<bool>());
  } else {
    result.set<bool>(lhs.get<bool>() || rhs.get<bool>());
    result.set_is_null(!lhs.get<bool>() && rhs.is_null());
  }
}

void jit_is_null(const JitMaterializedValue &lhs, JitMaterializedValue &result) {
  DebugAssert(result.data_type() == DataType::Bool, "invalid type for operation");

  result.set_is_null(false);
  result.set<bool>(lhs.is_null());
}

void jit_is_not_null(const JitMaterializedValue &lhs, JitMaterializedValue &result) {
  DebugAssert(result.data_type() == DataType::Bool, "invalid type for operation");

  result.set_is_null(false);
  result.set<bool>(!lhs.is_null());
}

} // namespace opossum
