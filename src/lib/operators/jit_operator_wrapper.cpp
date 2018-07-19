#include "jit_operator_wrapper.hpp"

#include "operators/jit_operator/operators/jit_aggregate.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_read_value.hpp"
#include "operators/jit_operator/operators/jit_validate.hpp"
#include "global.hpp"

namespace opossum {

JitOperatorWrapper::JitOperatorWrapper(const std::shared_ptr<const AbstractOperator> left,
                                       const JitExecutionMode execution_mode,
                                       const std::list<std::shared_ptr<AbstractJittable>>& jit_operators)
    : AbstractReadOnlyOperator{OperatorType::JitOperatorWrapper, left},
      _execution_mode{execution_mode},
      _jit_operators{jit_operators} {}

const std::string JitOperatorWrapper::name() const { return "JitOperatorWrapper"; }

const std::string JitOperatorWrapper::description(DescriptionMode description_mode) const {
  std::stringstream desc;
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  desc << "[JitOperatorWrapper]" << separator;
  for (const auto& op : _jit_operators) {
    desc << op->description() << separator;
  }
  return desc.str();
}

void JitOperatorWrapper::add_jit_operator(const std::shared_ptr<AbstractJittable>& op) { _jit_operators.push_back(op); }

const std::list<std::shared_ptr<AbstractJittable>>& JitOperatorWrapper::jit_operators() const {
  return _jit_operators;
}

const std::shared_ptr<JitReadTuples> JitOperatorWrapper::_source() const {
  return std::dynamic_pointer_cast<JitReadTuples>(_jit_operators.front());
}

const std::shared_ptr<AbstractJittableSink> JitOperatorWrapper::_sink() const {
  return std::dynamic_pointer_cast<AbstractJittableSink>(_jit_operators.back());
}

void JitOperatorWrapper::insert_loads(const bool lazy) {
  if (!lazy) {
    auto itr = ++_jit_operators.begin();
    for (size_t index = 0; index < _source()->input_columns().size(); ++index) {
      itr = _jit_operators.insert(itr, std::make_shared<JitReadValue>(_source()->input_columns()[index], index));
    }
    return;
  }
  std::map<size_t, size_t> inverted_input_columns;
  auto input_columns = _source()->input_columns();
  for (size_t input_column_index = 0; input_column_index < input_columns.size(); ++input_column_index) {
    inverted_input_columns[input_columns[input_column_index].tuple_value.tuple_index()] = input_column_index;
  }

  std::list<std::shared_ptr<AbstractJittable>> jit_operators;
  std::vector<std::map<size_t, bool>> accessed_column_ids;
  accessed_column_ids.reserve(jit_operators.size());
  std::map<size_t, bool> column_id_used_by_one_operator;
  for (const auto& jit_operator : _jit_operators) {
    auto col_ids = jit_operator->accessed_column_ids();
    accessed_column_ids.emplace_back(col_ids);
    for (const auto& pair : accessed_column_ids.back()) {
      if (inverted_input_columns.count(pair.first)) {
        column_id_used_by_one_operator[pair.first] = !column_id_used_by_one_operator.count(pair.first);
      }
    }
  }
  auto ids_itr = accessed_column_ids.begin();
  for (auto& jit_operator : _jit_operators) {
    for (const auto& pair : *ids_itr++) {
      if (column_id_used_by_one_operator.count(pair.first)) {
        if (pair.second && column_id_used_by_one_operator[pair.first]) {
          // insert within JitCompute operator
          auto compute_ptr = std::dynamic_pointer_cast<JitCompute>(jit_operator);
          compute_ptr->set_load_column(pair.first, inverted_input_columns[pair.first]);
        } else {
          jit_operators.emplace_back(std::make_shared<JitReadValue>(
              _source()->input_columns()[inverted_input_columns[pair.first]], inverted_input_columns[pair.first]));
        }
        column_id_used_by_one_operator.erase(pair.first);
      }
    }
    jit_operators.push_back(jit_operator);
  }

  _jit_operators = jit_operators;
}

std::shared_ptr<const Table> JitOperatorWrapper::_on_execute() {
  Assert(_source(), "JitOperatorWrapper does not have a valid source node.");
  Assert(_sink(), "JitOperatorWrapper does not have a valid sink node.");

  // const_cast<JitExecutionMode&>(_execution_mode) = JitExecutionMode::Interpret;

  // std::cout << "Before make loads lazy:" << std::endl << description(DescriptionMode::MultiLine) << std::endl;
  insert_loads(Global::get().lazy_load);
  // std::cout << "Specialising: " << (_execution_mode == JitExecutionMode::Compile ? "true" : "false") << std::endl;
  // std::cout << description(DescriptionMode::MultiLine) << std::endl;

  const auto& in_table = *input_left()->get_output();

  auto out_table = _sink()->create_output_table(in_table.max_chunk_size());

  JitRuntimeContext context;
  if (transaction_context_is_set()) {
    context.transaction_id = transaction_context()->transaction_id();
    context.snapshot_commit_id = transaction_context()->snapshot_commit_id();
  }
  _source()->before_query(in_table, context);
  _sink()->before_query(*out_table, context);

  // Connect operators to a chain
  for (auto it = _jit_operators.begin(), next = ++_jit_operators.begin(); it != _jit_operators.end() && next != _jit_operators.end(); ++it, ++next) {
    (*it)->set_next_operator(*(next));
  }

  std::function<void(const JitReadTuples*, JitRuntimeContext&)> execute_func;
  // We want to perform two specialization passes if the operator chain contains a JitAggregate operator, since the
  // JitAggregate operator contains multiple loops that need unrolling.
  auto two_specialization_passes = static_cast<bool>(std::dynamic_pointer_cast<JitAggregate>(_sink()));
  switch (_execution_mode) {  // _execution_mode
    case JitExecutionMode::Compile:
      // this corresponds to "opossum::JitReadTuples::execute(opossum::JitRuntimeContext&) const"
      execute_func = _module.specialize_and_compile_function<void(const JitReadTuples*, JitRuntimeContext&)>(
          "_ZNK7opossum13JitReadTuples7executeERNS_17JitRuntimeContextE",
          std::make_shared<JitConstantRuntimePointer>(_source().get()), two_specialization_passes);
      break;
    case JitExecutionMode::Interpret:
      execute_func = &JitReadTuples::execute;
      break;
  }

  for (opossum::ChunkID chunk_id{0}; chunk_id < in_table.chunk_count(); ++chunk_id) {
    const auto& in_chunk = *in_table.get_chunk(chunk_id);
    _source()->before_chunk(in_table, in_chunk, context);
    execute_func(_source().get(), context);
    _sink()->after_chunk(*out_table, context);
  }

  _sink()->after_query(*out_table, context);

  return out_table;
}

std::shared_ptr<AbstractOperator> JitOperatorWrapper::_on_recreate(
    const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {
  return std::make_shared<JitOperatorWrapper>(recreated_input_left, _execution_mode, _jit_operators);
}

}  // namespace opossum
