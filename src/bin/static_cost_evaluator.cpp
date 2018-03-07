#include <chrono>
#include <iostream>

#include "operators/table_wrapper.hpp"
#include "operators/table_scan.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/join_hash.hpp"
#include "operators/product.hpp"
#include "utils/table_generator2.hpp"
#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "constant_mappings.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "tpch/tpch_queries.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "planviz/lqp_visualizer.hpp"

using namespace opossum; // NOLINT

class TableScanCostModel {
 public:
#if IS_DEBUG
  TableScanCostModel(const float scan_cost_factor = 0.2f, const float output_cost_factor = 1.6f):
#else
  TableScanCostModel(const float scan_cost_factor = 0.001f, const float output_cost_factor = 0.008f):
#endif
    _scan_cost_factor(scan_cost_factor), _output_cost_factor(output_cost_factor) {}

  float estimate_cost(TableStatistics& table_statistics, const ColumnID column_id, const PredicateCondition predicate_condition, const AllTypeVariant& value) const {
    const auto predicated_statistics = table_statistics.predicate_statistics(column_id, predicate_condition, value);

    const auto scan_cost = table_statistics.row_count() * _scan_cost_factor;
    const auto output_cost = predicated_statistics->row_count() * _output_cost_factor;

    return scan_cost + output_cost;
  }

 private:
  float _scan_cost_factor;
  float _output_cost_factor;
};

template<typename Fn>
std::chrono::high_resolution_clock::duration time_fn(const Fn& fn) {
  const auto begin = std::chrono::high_resolution_clock::now();
  fn();
  const auto end = std::chrono::high_resolution_clock::now();
  return end - begin;
}
//
//class BaseCostEvaluator {
// public:
//  virtual ~BaseCostEvaluator() = default;
//
//  virtual std::shared_ptr<AbstractOperator> create_operator() const = 0;
//  virtual float estimate_cost() const = 0;
//  virtual std::shared_ptr<TableStatistics> estimate_output() const = 0;
//
//  void operator()() {
//    _iterate_left_column_ids();
//  }
//
//  void set_left_input_params(const std::vector<TableGenerator2ColumnDefinitions>& column_definitions, const std::vector<size_t>& row_counts_per_chunk, const std::vector<size_t>& chunk_counts) {
//    _left_column_definitions = column_definitions;
//    _left_row_counts_per_chunk = row_counts_per_chunk;
//    _left_chunk_counts = chunk_counts;
//  }
//
//  void set_left_column_ids(const std::vector<ColumnID>& left_column_ids) {
//    _left_column_ids = left_column_ids;
//  }
//
//  void set_predicate_conditions(const std::vector<PredicateCondition>& predicate_conditions) {
//    _predicate_conditions = predicate_conditions;
//  }
//
//  void set_values(const std::vector<AllTypeVariant> values) {
//    _values = values;
//  }
//
// protected:
//  void _iterate_left_column_ids() {
//    if (_left_column_ids) {
//      for (const auto& left_column_id : *_left_column_ids) {
//        _left_column_id = left_column_id;
//        _iterate_predicate_conditions();
//      }
//    } else {
//      _iterate_predicate_conditions();
//    }
//  }
//
//  void _iterate_predicate_conditions() {
//    if (_predicate_conditions) {
//      for (const auto& predicate_condition : *_predicate_conditions) {
//        _predicate_condition = predicate_condition;
//        _iterate_values();
//      }
//    } else {
//      _iterate_values();
//    }
//  }
//
//  void _iterate_values() {
//    if (_values) {
//      for (const auto& value : *_values) {
//        _value = value;
//        _evaluate();
//      }
//    } else {
//      _evaluate();
//    }
//  }
//
//  void _evaluate() {
//    for (const auto& left_column_definitions : _left_column_definitions) {
//      for (const auto &left_row_count_per_chunk : _left_row_counts_per_chunk) {
//        for (const auto &left_chunk_count : _left_chunk_counts) {
//          std::cout << "C: " << left_column_definitions.size() << ", ";
//          std::cout << "LCS: " << left_row_count_per_chunk << ", ";
//          std::cout << "LCC: " << left_chunk_count << ", ";
//          if (_left_column_id) std::cout << "LCID: " << _left_column_id.value() << ",";
//          if (_predicate_condition)
//            std::cout << "PC: " << predicate_condition_to_string.left.at(_predicate_condition.value()) << ",";
//          if (_value) std::cout << "V: " << _value.value() << ",";
//
//          const auto left_table_generator = TableGenerator2(left_column_definitions, left_row_count_per_chunk,
//                                                            left_chunk_count);
//          _left_input = std::make_shared<TableWrapper>(left_table_generator.generate_table());
//          _left_input_statistics = std::make_shared<TableStatistics>(
//          std::const_pointer_cast<Table>(_left_input->get_output()));
//
//          const auto op = create_operator();
//          const auto duration = time_fn([&]() { op->execute(); });
//          const auto estimated_selectivity =
//          static_cast<float>(estimate_output()->row_count()) / (left_row_count_per_chunk * left_chunk_count);
//          const auto selectivity =
//          static_cast<float>(op->get_output()->row_count()) / (left_row_count_per_chunk * left_chunk_count);
//          std::cout << " -- S: " << selectivity << "; ES: " << estimated_selectivity << "; D: "
//                    << std::chrono::duration_cast<std::chrono::microseconds>(duration).count() << "; E: "
//                    << estimate_cost() << std::endl;
//        }
//      }
//    }
//  }
//
//  std::vector<TableGenerator2ColumnDefinitions> _left_column_definitions;
//  std::vector<size_t> _left_row_counts_per_chunk;
//  std::vector<size_t> _left_chunk_counts;
//
//  std::shared_ptr<AbstractOperator> _left_input;
//  std::shared_ptr<TableStatistics> _left_input_statistics;
//
//  std::optional<std::vector<ColumnID>> _left_column_ids;
//  std::optional<std::vector<PredicateCondition>> _predicate_conditions;
//  std::optional<std::vector<AllTypeVariant>> _values;
//
//  std::optional<ColumnID> _left_column_id;
//  std::optional<PredicateCondition> _predicate_condition;
//  std::optional<AllTypeVariant> _value;
//};
//
//class TableScanEvaluator : public BaseCostEvaluator {
//  std::shared_ptr<AbstractOperator> create_operator() const {
//    return std::make_shared<TableScan>(_left_input, _left_column_id.value(), _predicate_condition.value(), _value.value());
//  }
//
//  float estimate_cost() const {
//    return TableScanCostModel{}.estimate_cost(*_left_input_statistics, _left_column_id.value(), _predicate_condition.value(), _value.value());
//  }
//
//  std::shared_ptr<TableStatistics> estimate_output() const override {
//    return _left_input_statistics->predicate_statistics(_left_column_id.value(), _predicate_condition.value(), _value.value());
//  }
//};

std::vector<std::shared_ptr<AbstractOperator>> flatten_pqp_impl(const std::shared_ptr<AbstractOperator>& pqp, std::unordered_set<std::shared_ptr<AbstractOperator>>& enlisted_operators) {
  if (enlisted_operators.count(pqp) > 0) return {};
  enlisted_operators.insert(pqp);

  std::vector<std::shared_ptr<AbstractOperator>> operators;

  if (!pqp->input_left()) return {pqp};

  auto left_flattened = flatten_pqp_impl(std::const_pointer_cast<AbstractOperator>(pqp->input_left()), enlisted_operators);

  if (pqp->input_right()) {
    auto right_flattened = flatten_pqp_impl(std::const_pointer_cast<AbstractOperator>(pqp->input_right()), enlisted_operators);
    left_flattened.insert(left_flattened.end(), right_flattened.begin(), right_flattened.end());
  }

  left_flattened.emplace_back(pqp);

  return left_flattened;
}

std::vector<std::shared_ptr<AbstractOperator>> flatten_pqp(const std::shared_ptr<AbstractOperator>& pqp) {
  std::unordered_set<std::shared_ptr<AbstractOperator>> enlisted_operators;
  return flatten_pqp_impl(pqp, enlisted_operators);
}

struct TableScanSample final {
  TableScanSample(const size_t input_row_count, const size_t output_row_count, const long microseconds):
    input_row_count(input_row_count), output_row_count(output_row_count), microseconds(microseconds) {}

  size_t input_row_count{0};
  size_t output_row_count{0};

  long microseconds{0};

  friend std::ostream& operator<<(std::ostream& stream, const TableScanSample& sample) {
    stream << sample.input_row_count << ";" << sample.output_row_count << ";" << sample.microseconds;
    return stream;
  }
};

struct JoinHashSample final {
  JoinHashSample(const std::string& info,
                 const size_t left_input_row_count,
                 const size_t right_input_row_count,
                 const DataType left_data_type,
                 const DataType right_data_type,
                 const size_t output_row_count,
                 const long microseconds):
  info(info),
  left_input_row_count(left_input_row_count),
  right_input_row_count(right_input_row_count),
  left_data_type(left_data_type),
  right_data_type(right_data_type),
  output_row_count(output_row_count),
  microseconds(microseconds) {}

  std::string info;

  size_t left_input_row_count{0};
  size_t right_input_row_count{0};

  DataType left_data_type{DataType::Int};
  DataType right_data_type{DataType::Int};

  size_t output_row_count{0};

  long microseconds{0};

  friend std::ostream& operator<<(std::ostream& stream, const JoinHashSample& sample) {
    stream
           << sample.info << ";"
           << data_type_to_string.left.at(sample.left_data_type) << ";"
           << data_type_to_string.left.at(sample.right_data_type) << ";"
           << sample.left_input_row_count << ";"
           << sample.right_input_row_count << ";"
           << sample.output_row_count << ";"
           << sample.microseconds;
    return stream;
  }
};

std::vector<TableScanSample> table_scan_samples;
std::vector<JoinHashSample> join_hash_samples;

void visit_table_scan(TableScan& table_scan) {
  const auto row_count = table_scan.input_table_left()->row_count();

  const auto duration = time_fn([&]() {table_scan.execute();});

  table_scan_samples.emplace_back(row_count, table_scan.get_output()->row_count(), std::chrono::duration_cast<std::chrono::microseconds>(duration).count());
}

void visit_join_hash(JoinHash& join_hash) {
  const auto left_row_count = join_hash.input_table_left()->row_count();
  const auto right_row_count = join_hash.input_table_right()->row_count();

  const auto duration = time_fn([&]() {join_hash.execute();});

  join_hash_samples.emplace_back(join_hash.description(DescriptionMode::SingleLine),
  left_row_count,
                                 right_row_count,
                                 join_hash.input_table_left()->column_data_type(join_hash.column_ids().first),
                                 join_hash.input_table_right()->column_data_type(join_hash.column_ids().second),
                                 join_hash.get_output()->row_count(),
                                 std::chrono::duration_cast<std::chrono::microseconds>(duration).count());
}

void visit_op(const std::shared_ptr<AbstractOperator>& op) {
  if (const auto table_scan = std::dynamic_pointer_cast<TableScan>(op)) {
    visit_table_scan(*table_scan);
  } else if (const auto join_hash = std::dynamic_pointer_cast<JoinHash>(op)) {
    visit_join_hash(*join_hash);
  } else {
    op->execute();
  }
}

int main() {
  std::cout << "-- Static Cost Evaluator" << std::endl;

  const auto sql_queries = std::vector<std::string>({
    tpch_queries[6]
  });

  for (const auto scale_factor : {0.1f}) {
    std::cout << "ScaleFactor: " << scale_factor << std::endl;

    TpchDbGenerator{scale_factor}.generate_and_store();

    for (const auto& sql_query : sql_queries) {
      SQLPipelineStatement statement{sql_query};
      const auto pqp = statement.get_query_plan()->tree_roots().at(0);
      const auto operators = flatten_pqp(pqp);

      for (const auto& op : operators) {
        visit_op(op);
      }

      GraphvizConfig graphviz_config;
      graphviz_config.format = "svg";

      VizGraphInfo graph_info;
      graph_info.bg_color = "black";

      auto prefix = std::string("plan");
      SQLQueryPlanVisualizer{graphviz_config, graph_info, {}, {}}.visualize(*statement.get_query_plan(), prefix + ".dot", prefix + ".svg");
    }

    StorageManager::reset();
  }

  for (const auto sample : join_hash_samples) {
    std::cout << sample << std::endl;
  }
}
