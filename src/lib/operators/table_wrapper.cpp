#include "table_wrapper.hpp"

#include <memory>
#include <string>
#include <vector>

namespace opossum {

TableWrapper::TableWrapper(const std::shared_ptr<const Table> table) : _table(table) {
  _output = _table; // No need to call execute() on a TableWrapper as it is just a Debug/Test tool
}

const std::string TableWrapper::name() const { return "TableWrapper"; }

std::shared_ptr<AbstractOperator> TableWrapper::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<TableWrapper>(_table);
}

std::shared_ptr<const Table> TableWrapper::_on_execute() { return _table; }
}  // namespace opossum
