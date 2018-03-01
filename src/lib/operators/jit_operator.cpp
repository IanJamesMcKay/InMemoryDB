#include "jit_operator.hpp"

#include "jit_evaluation_helper.hpp"

namespace opossum {

JitOperator::JitOperator(const std::shared_ptr<const AbstractOperator> left, const bool use_jit,
                         const std::vector<std::shared_ptr<JitAbstractOperator>>& operators)
    : AbstractReadOnlyOperator{left}, _use_jit{use_jit}, _operators{operators}, _module{"_ZNK7opossum12JitReadTuple7executeERNS_17JitRuntimeContextE"} {}

const std::string JitOperator::name() const { return "JitOperator"; }

const std::string JitOperator::description(DescriptionMode description_mode) const {
  std::stringstream desc;
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  desc << "[JitOperator]" << separator;
  for (const auto& op : _operators) {
    desc << op->description() << separator;
  }
  return desc.str();
}

std::shared_ptr<AbstractOperator> JitOperator::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<JitOperator>(input_left()->recreate(args), _use_jit, _operators);
}

void JitOperator::add_jit_operator(const std::shared_ptr<JitAbstractOperator>& op) { _operators.push_back(op); }

const std::shared_ptr<JitReadTuple> JitOperator::_source() const {
  return std::dynamic_pointer_cast<JitReadTuple>(_operators.front());
}

const std::shared_ptr<JitAbstractSink> JitOperator::_sink() const {
  return std::dynamic_pointer_cast<JitAbstractSink>(_operators.back());
}

void JitOperator::_prepare() {
  // Connect operators to a chain
  for (auto it = _operators.begin(); it != _operators.end() && it + 1 != _operators.end(); ++it) {
    (*it)->set_next_operator(*(it + 1));
  }

  DebugAssert(_source(), "JitOperator does not have a valid source node.");
  DebugAssert(_sink(), "JitOperator does not have a valid sink node.");

  if (JitEvaluationHelper::get().experiment().at("jit_engine") == "none") {
    _execute_func = &JitReadTuple::execute;
  } else if (JitEvaluationHelper::get().experiment().at("jit_engine") == "slow") {
    auto start = std::chrono::high_resolution_clock::now();
    _module.specialize_slow(std::make_shared<JitConstantRuntimePointer>(_source().get()));
    auto runtime = std::round(
            std::chrono::duration<double, std::micro>(std::chrono::high_resolution_clock::now() - start).count());
    _execute_func = _module.compile<void(const JitReadTuple*, JitRuntimeContext&)>();
    std::cerr << "slow jitting took " << runtime / 1000.0 << "ms" << std::endl;
  } else if (JitEvaluationHelper::get().experiment().at("jit_engine") == "fast") {
    auto start = std::chrono::high_resolution_clock::now();
    _module.specialize_fast(std::make_shared<JitConstantRuntimePointer>(_source().get()));
    auto runtime = std::round(
            std::chrono::duration<double, std::micro>(std::chrono::high_resolution_clock::now() - start).count());
    _execute_func = _module.compile<void(const JitReadTuple*, JitRuntimeContext&)>();
    std::cerr << "fast jitting took " << runtime / 1000.0 << "ms" << std::endl;
  } else {
    Fail("unknown jet engine");
  }
}

std::shared_ptr<const Table> JitOperator::_on_execute() {
  const auto& in_table = *input_left()->get_output();
  auto out_table = std::make_shared<opossum::Table>(in_table.max_chunk_size());

  JitRuntimeContext context;
  _source()->before_query(in_table, context);
  _sink()->before_query(*out_table, context);

  auto start = std::chrono::high_resolution_clock::now();
  for (opossum::ChunkID chunk_id{0}; chunk_id < in_table.chunk_count(); ++chunk_id) {
    const auto& in_chunk = *in_table.get_chunk(chunk_id);

    context.chunk_size = in_chunk.size();
    context.chunk_offset = 0;

    _source()->before_chunk(in_table, in_chunk, context);
    _execute_func(_source().get(), context);
    _sink()->after_chunk(*out_table, context);
  }

  _sink()->after_query(*out_table, context);

  auto runtime =
      std::round(std::chrono::duration<double, std::micro>(std::chrono::high_resolution_clock::now() - start).count());
  std::cerr << "running took " << runtime / 1000.0 << "ms" << std::endl;

  return out_table;
}

// cleanup
#undef JIT_ABSTRACT_SOURCE_EXECUTE_MANGLED_NAME

}  // namespace opossum
