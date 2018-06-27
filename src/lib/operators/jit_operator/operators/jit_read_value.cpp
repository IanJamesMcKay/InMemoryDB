#include "jit_read_value.hpp"

namespace opossum {

std::string JitReadValue::description() const {
  std::stringstream desc;
  desc << "[ReadValue] x" << _input_column.tuple_value.tuple_index() << " = Col#" << _input_column.column_id << ", ";
  return desc.str();
}

void JitReadValue::_consume(JitRuntimeContext& context) const {
  context.inputs[_input_column_index]->read_value(context);
  _emit(context);
}

}  // namespace opossum
