#include <chrono>
#include <iostream>

#include "operators/table_wrapper.hpp"
#include "operators/table_scan.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/join_hash.hpp"
#include "operators/product.hpp"
#include "utils/table_generator2.hpp"
#include "optimizer/table_statistics.hpp"
#include "constant_mappings.hpp"

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

class BaseCostEvaluator {
 public:
  virtual ~BaseCostEvaluator() = default;

  virtual std::shared_ptr<AbstractOperator> create_operator() const = 0;
  virtual float estimate_cost() const = 0;
  virtual std::shared_ptr<TableStatistics> estimate_output() const = 0;

  void operator()() {
    _iterate_left_column_ids();
  }

  void set_left_input_params(const std::vector<TableGenerator2ColumnDefinitions>& column_definitions, const std::vector<size_t>& row_counts_per_chunk, const std::vector<size_t>& chunk_counts) {
    _left_column_definitions = column_definitions;
    _left_row_counts_per_chunk = row_counts_per_chunk;
    _left_chunk_counts = chunk_counts;
  }

  void set_left_column_ids(const std::vector<ColumnID>& left_column_ids) {
    _left_column_ids = left_column_ids;
  }

  void set_predicate_conditions(const std::vector<PredicateCondition>& predicate_conditions) {
    _predicate_conditions = predicate_conditions;
  }

  void set_values(const std::vector<AllTypeVariant> values) {
    _values = values;
  }

 protected:
  void _iterate_left_column_ids() {
    if (_left_column_ids) {
      for (const auto& left_column_id : *_left_column_ids) {
        _left_column_id = left_column_id;
        _iterate_predicate_conditions();
      }
    } else {
      _iterate_predicate_conditions();
    }
  }

  void _iterate_predicate_conditions() {
    if (_predicate_conditions) {
      for (const auto& predicate_condition : *_predicate_conditions) {
        _predicate_condition = predicate_condition;
        _iterate_values();
      }
    } else {
      _iterate_values();
    }
  }

  void _iterate_values() {
    if (_values) {
      for (const auto& value : *_values) {
        _value = value;
        _evaluate();
      }
    } else {
      _evaluate();
    }
  }

  void _evaluate() {
    for (const auto& left_column_definitions : _left_column_definitions) {
      for (const auto &left_row_count_per_chunk : _left_row_counts_per_chunk) {
        for (const auto &left_chunk_count : _left_chunk_counts) {
          std::cout << "C: " << left_column_definitions.size() << ", ";
          std::cout << "LCS: " << left_row_count_per_chunk << ", ";
          std::cout << "LCC: " << left_chunk_count << ", ";
          if (_left_column_id) std::cout << "LCID: " << _left_column_id.value() << ",";
          if (_predicate_condition)
            std::cout << "PC: " << predicate_condition_to_string.left.at(_predicate_condition.value()) << ",";
          if (_value) std::cout << "V: " << _value.value() << ",";

          const auto left_table_generator = TableGenerator2(left_column_definitions, left_row_count_per_chunk,
                                                            left_chunk_count);
          _left_input = std::make_shared<TableWrapper>(left_table_generator.generate_table());
          _left_input_statistics = std::make_shared<TableStatistics>(
          std::const_pointer_cast<Table>(_left_input->get_output()));

          const auto op = create_operator();
          const auto duration = time_fn([&]() { op->execute(); });
          const auto estimated_selectivity =
          static_cast<float>(estimate_output()->row_count()) / (left_row_count_per_chunk * left_chunk_count);
          const auto selectivity =
          static_cast<float>(op->get_output()->row_count()) / (left_row_count_per_chunk * left_chunk_count);
          std::cout << " -- S: " << selectivity << "; ES: " << estimated_selectivity << "; D: "
                    << std::chrono::duration_cast<std::chrono::microseconds>(duration).count() << "; E: "
                    << estimate_cost() << std::endl;
        }
      }
    }
  }

  std::vector<TableGenerator2ColumnDefinitions> _left_column_definitions;
  std::vector<size_t> _left_row_counts_per_chunk;
  std::vector<size_t> _left_chunk_counts;

  std::shared_ptr<AbstractOperator> _left_input;
  std::shared_ptr<TableStatistics> _left_input_statistics;

  std::optional<std::vector<ColumnID>> _left_column_ids;
  std::optional<std::vector<PredicateCondition>> _predicate_conditions;
  std::optional<std::vector<AllTypeVariant>> _values;

  std::optional<ColumnID> _left_column_id;
  std::optional<PredicateCondition> _predicate_condition;
  std::optional<AllTypeVariant> _value;
};

class TableScanEvaluator : public BaseCostEvaluator {
  std::shared_ptr<AbstractOperator> create_operator() const {
    return std::make_shared<TableScan>(_left_input, _left_column_id.value(), _predicate_condition.value(), _value.value());
  }

  float estimate_cost() const {
    return TableScanCostModel{}.estimate_cost(*_left_input_statistics, _left_column_id.value(), _predicate_condition.value(), _value.value());
  }

  std::shared_ptr<TableStatistics> estimate_output() const override {
    return _left_input_statistics->predicate_statistics(_left_column_id.value(), _predicate_condition.value(), _value.value());
  }
};

int main() {
  std::cout << "-- Static Cost Evaluator" << std::endl;

  TableGenerator2ColumnDefinitions column_definitions_a;
  column_definitions_a.emplace_back(DataType::Int, 0, 1'000);
  column_definitions_a.emplace_back(DataType::Int, 0, 50'000);
  column_definitions_a.emplace_back(DataType::Int, 0, 1'000'000);
  column_definitions_a.emplace_back(DataType::Float, 0.0f, 100'000.0f);

  TableGenerator2ColumnDefinitions column_definitions_b;
  column_definitions_b.emplace_back(DataType::Int, 0, 1'000);
  column_definitions_b.emplace_back(DataType::Int, 0, 1'000);
  column_definitions_b.emplace_back(DataType::Int, 0, 1'000);
  column_definitions_b.emplace_back(DataType::Int, 0, 50'000);
  column_definitions_b.emplace_back(DataType::Int, 0, 50'000);
  column_definitions_b.emplace_back(DataType::Int, 0, 1'000'000);
  column_definitions_b.emplace_back(DataType::Int, 0, 1'000'000);
  column_definitions_b.emplace_back(DataType::Float, 0.0f, 100'000.0f);

  TableScanEvaluator table_scan_evaluator;
  table_scan_evaluator.set_left_input_params({column_definitions_a, column_definitions_b}, {1'000, 20'000, 100'000, 500'000, 1'500'000}, {5});
  table_scan_evaluator.set_left_column_ids({ColumnID{0}, ColumnID{3}});
  table_scan_evaluator.set_predicate_conditions({PredicateCondition::Equals, PredicateCondition::LessThan});
  table_scan_evaluator.set_values({AllTypeVariant(100), AllTypeVariant{10'000}, AllTypeVariant{500'000}});
  table_scan_evaluator();
}
