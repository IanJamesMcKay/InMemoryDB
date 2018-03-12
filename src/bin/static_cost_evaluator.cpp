#include <array>
#include <chrono>
#include <fstream>
#include <iostream>

#include "constant_mappings.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/product.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "optimizer/join_ordering/abstract_join_plan_node.hpp"
#include "optimizer/join_ordering/cost_model_naive.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/dp_ccp_top_k.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "optimizer/table_statistics.hpp"
#include "planviz/lqp_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"
#include "utils/table_generator2.hpp"

using namespace opossum;  // NOLINT

template <typename Fn>
std::chrono::high_resolution_clock::duration time_fn(const Fn& fn) {
  const auto begin = std::chrono::high_resolution_clock::now();
  fn();
  const auto end = std::chrono::high_resolution_clock::now();
  return end - begin;
}

std::vector<std::shared_ptr<AbstractOperator>> flatten_pqp_impl(
    const std::shared_ptr<AbstractOperator>& pqp,
    std::unordered_set<std::shared_ptr<AbstractOperator>>& enlisted_operators) {
  if (enlisted_operators.count(pqp) > 0) return {};
  enlisted_operators.insert(pqp);

  std::vector<std::shared_ptr<AbstractOperator>> operators;

  if (!pqp->input_left()) return {pqp};

  auto left_flattened =
      flatten_pqp_impl(std::const_pointer_cast<AbstractOperator>(pqp->input_left()), enlisted_operators);

  if (pqp->input_right()) {
    auto right_flattened =
        flatten_pqp_impl(std::const_pointer_cast<AbstractOperator>(pqp->input_right()), enlisted_operators);
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
  TableScanSample(const size_t input_row_count, const size_t output_row_count, const long microseconds)
      : input_row_count(input_row_count), output_row_count(output_row_count), microseconds(microseconds) {}

  size_t input_row_count{0};
  size_t output_row_count{0};

  long microseconds{0};

  friend std::ostream& operator<<(std::ostream& stream, const TableScanSample& sample) {
    stream << sample.input_row_count << ";" << sample.output_row_count << ";" << sample.microseconds;
    return stream;
  }
};

struct JoinHashSample final {
  JoinHashSample(const std::string& info, const JoinHashPerformanceData& performance_data,
                 const size_t left_input_row_count, const size_t right_input_row_count, const DataType left_data_type,
                 const DataType right_data_type, const size_t output_row_count)
      : info(info),
        performance_data(performance_data),
        left_input_row_count(left_input_row_count),
        right_input_row_count(right_input_row_count),
        left_data_type(left_data_type),
        right_data_type(right_data_type),
        output_row_count(output_row_count) {}

  std::string info;

  JoinHashPerformanceData performance_data;

  size_t left_input_row_count{0};
  size_t right_input_row_count{0};

  DataType left_data_type{DataType::Int};
  DataType right_data_type{DataType::Int};

  size_t output_row_count{0};

  friend std::ostream& operator<<(std::ostream& stream, const JoinHashSample& sample) {
    stream << data_type_to_string.left.at(sample.left_data_type) << ";"
           << data_type_to_string.left.at(sample.right_data_type) << ";" << sample.left_input_row_count << ";"
           << sample.right_input_row_count << ";" << sample.output_row_count << ";"
           << sample.performance_data.materialization.count() / 1000 << ";"
           << sample.performance_data.partitioning.count() / 1000 << ";" << sample.performance_data.probe.count() / 1000
           << ";" << sample.performance_data.build.count() / 1000 << ";"
           << sample.performance_data.output.count() / 1000 << ";" << sample.performance_data.total.count() / 1000
           << ";";
    return stream;
  }
};

struct JoinSortMergeSample final {
  JoinSortMergeSample(const std::string& info, const BaseOperatorPerformanceData& performance_data,
                      const size_t left_input_row_count, const size_t right_input_row_count,
                      const size_t output_row_count)
      : info(info),
        performance_data(performance_data),
        left_input_row_count(left_input_row_count),
        right_input_row_count(right_input_row_count),
        output_row_count(output_row_count) {}

  std::string info;

  BaseOperatorPerformanceData performance_data;

  size_t left_input_row_count{0};
  size_t right_input_row_count{0};

  size_t output_row_count{0};

  friend std::ostream& operator<<(std::ostream& stream, const JoinSortMergeSample& sample) {
    stream << sample.info << ";" << sample.left_input_row_count << ";" << sample.right_input_row_count << ";"
           << sample.output_row_count << ";"
           << "  " << sample.performance_data.total.count() / 1000 << ";";
    return stream;
  }
};

struct ProductSample final {
  ProductSample(const BaseOperatorPerformanceData& performance_data, const size_t total_column_count,
                const size_t product_row_count)
      : performance_data(performance_data),
        total_column_count(total_column_count),
        product_row_count(product_row_count) {}

  BaseOperatorPerformanceData performance_data;
  size_t total_column_count;
  size_t product_row_count;

  friend std::ostream& operator<<(std::ostream& stream, const ProductSample& sample) {
    stream << sample.total_column_count << ";" << sample.product_row_count << ";"
           << sample.performance_data.total.count() / 1000 << ";";
    return stream;
  }
};

std::vector<TableScanSample> table_scan_samples;
std::vector<JoinHashSample> join_hash_samples;
std::vector<ProductSample> product_samples;
std::vector<JoinSortMergeSample> join_sort_merge_samples;

void visit_table_scan(TableScan& table_scan) {
  const auto row_count = table_scan.input_table_left()->row_count();

  const auto duration = time_fn([&]() { table_scan.execute(); });

  table_scan_samples.emplace_back(row_count, table_scan.get_output()->row_count(),
                                  std::chrono::duration_cast<std::chrono::microseconds>(duration).count());
}

void visit_join_hash(JoinHash& join_hash) {
  const auto left_row_count = join_hash.input_table_left()->row_count();
  const auto right_row_count = join_hash.input_table_right()->row_count();

  join_hash.execute();

  join_hash_samples.emplace_back(join_hash.description(DescriptionMode::SingleLine), join_hash.performance_data(),
                                 left_row_count, right_row_count,
                                 join_hash.input_table_left()->column_data_type(join_hash.column_ids().first),
                                 join_hash.input_table_right()->column_data_type(join_hash.column_ids().second),
                                 join_hash.get_output()->row_count());
}

void visit_join_sort_merge(JoinSortMerge& join_sort_merge) {
  const auto left_row_count = join_sort_merge.input_table_left()->row_count();
  const auto right_row_count = join_sort_merge.input_table_right()->row_count();

  join_sort_merge.execute();

  join_sort_merge_samples.emplace_back(join_sort_merge.description(DescriptionMode::SingleLine),
                                       join_sort_merge.performance_data(), left_row_count, right_row_count,
                                       join_sort_merge.get_output()->row_count());
}

void visit_product(Product& product) {
  const auto left_row_count = product.input_table_left()->row_count();
  const auto right_row_count = product.input_table_right()->row_count();

  product.execute();

  product_samples.emplace_back(product.performance_data(),
                               product.input_table_left()->column_count() + product.input_table_right()->column_count(),
                               left_row_count * right_row_count);
}

void visit_op(const std::shared_ptr<AbstractOperator>& op) {
  if (const auto table_scan = std::dynamic_pointer_cast<TableScan>(op)) {
    visit_table_scan(*table_scan);
  } else if (const auto join_hash = std::dynamic_pointer_cast<JoinHash>(op)) {
    visit_join_hash(*join_hash);
  } else if (const auto join_sort_merge = std::dynamic_pointer_cast<JoinSortMerge>(op)) {
    visit_join_sort_merge(*join_sort_merge);
  } else if (const auto product = std::dynamic_pointer_cast<Product>(op)) {
    visit_product(*product);
  } else {
    op->execute();
  }
}

int main() {
  std::cout << "-- Static Cost Evaluator" << std::endl;

  const auto sql_queries =
      std::vector<std::string>({tpch_queries[4], tpch_queries[5], tpch_queries[6], tpch_queries[8], tpch_queries[9]});

  //  auto optimizer = std::make_shared<Optimizer>(1);
  //  RuleBatch rule_batch(RuleBatchExecutionPolicy::Once);
  //  rule_batch.add_rule(std::make_shared<JoinOrderingRule>(std::make_shared<DpCcp>()));
  //  optimizer->add_rule_batch(rule_batch);

  for (const auto scale_factor : {0.001f, 0.005f, 0.01f, 0.02f}) {
    std::cout << "ScaleFactor: " << scale_factor << std::endl;

    TpchDbGenerator{scale_factor}.generate_and_store();

    for (const auto& sql_query : sql_queries) {
      SQLPipelineStatement statement{sql_query, UseMvcc::No};

      const auto lqp = LogicalPlanRootNode::make(statement.get_optimized_logical_plan());
      const auto join_graph = JoinGraph::from_lqp(lqp->left_input());

      DpCcpTopK dp_ccp_top_k{8, std::make_shared<CostModelNaive>()};

      dp_ccp_top_k(join_graph);

      JoinVertexSet all_vertices{join_graph->vertices.size()};
      all_vertices.flip();

      const auto join_plans = dp_ccp_top_k.subplan_cache()->get_best_plans(all_vertices);

      for (const auto& join_plan : join_plans) {
        const auto join_ordered_sub_lqp = join_plan->to_lqp();

        for (const auto& parent_relation : join_graph->output_relations) {
          parent_relation.output->set_input(parent_relation.input_side, join_ordered_sub_lqp);
        }

        const auto pqp = LQPTranslator{}.translate_node(lqp->left_input());

        const auto operators = flatten_pqp(pqp);

        for (const auto& op : operators) {
          visit_op(op);
        }

        //        GraphvizConfig graphviz_config;
        //        graphviz_config.format = "svg";

        //        VizGraphInfo graph_info;
        //        graph_info.bg_color = "black";

        //        auto prefix = std::string("plan");
        //        SQLQueryPlanVisualizer{graphviz_config, graph_info, {}, {}}.visualize(*statement.get_query_plan(),
        //                                                                              prefix + ".dot", prefix + ".svg");
      }
    }

    StorageManager::reset();
  }

  std::ofstream join_hash_csv("join_hash_0.csv");
  for (const auto sample : join_hash_samples) {
    join_hash_csv << sample << std::endl;
  }

  std::ofstream table_scan_csv("table_scan_0.csv");
  for (const auto sample : table_scan_samples) {
    table_scan_csv << sample << std::endl;
  }

  std::ofstream product_csv("product_0.csv");
  for (const auto sample : product_samples) {
    product_csv << sample << std::endl;
  }
}
