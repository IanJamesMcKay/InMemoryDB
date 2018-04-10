#include "gtest/gtest.h"

#include "statistics/column_statistics.hpp"
#include "statistics/generate_table_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/load_table.hpp"

namespace opossum {

#define EXPECT_INT_COLUMN_STATISTICS(column_statistics, null_value_ratio_, distinct_count_, min_, max_)  \
  ASSERT_EQ(column_statistics->data_type(), DataType::Int);                                              \
  EXPECT_FLOAT_EQ(column_statistics->null_value_ratio(), null_value_ratio_);                             \
  EXPECT_FLOAT_EQ(column_statistics->distinct_count(), distinct_count_);                                 \
  EXPECT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<int32_t>>(column_statistics)->min(), min_); \
  EXPECT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<int32_t>>(column_statistics)->max(), max_);

#define EXPECT_FLOAT_COLUMN_STATISTICS(column_statistics, null_value_ratio_, distinct_count_, min_, max_)    \
  ASSERT_EQ(column_statistics->data_type(), DataType::Float);                                                \
  EXPECT_FLOAT_EQ(column_statistics->null_value_ratio(), null_value_ratio_);                                 \
  EXPECT_FLOAT_EQ(column_statistics->distinct_count(), distinct_count_);                                     \
  EXPECT_FLOAT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<float>>(column_statistics)->min(), min_); \
  EXPECT_FLOAT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<float>>(column_statistics)->max(), max_);

#define EXPECT_STRING_COLUMN_STATISTICS(column_statistics, null_value_ratio_, distinct_count_, min_, max_)   \
  ASSERT_EQ(column_statistics->data_type(), DataType::String);                                               \
  EXPECT_FLOAT_EQ(column_statistics->null_value_ratio(), null_value_ratio_);                                 \
  EXPECT_FLOAT_EQ(column_statistics->distinct_count(), distinct_count_);                                     \
  EXPECT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<std::string>>(column_statistics)->min(), min_); \
  EXPECT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<std::string>>(column_statistics)->max(), max_);

class GenerateStatisticsTest : public ::testing::Test {};

TEST_F(GenerateStatisticsTest, GenerateTableStatisticsAllColumnTypes) {
  const auto table = load_table("src/test/tables/tpch/sf-0.001/customer.tbl");
  const auto table_statistics = generate_table_statistics(*table);

  ASSERT_EQ(table_statistics.column_statistics().size(), 8u);
  EXPECT_EQ(table_statistics.row_count(), 150u);

  EXPECT_INT_COLUMN_STATISTICS(table_statistics.column_statistics().at(0), 0.0f, 150, 1, 150);
  EXPECT_STRING_COLUMN_STATISTICS(table_statistics.column_statistics().at(1), 0.0f, 150, "Customer#000000001",
                                  "Customer#000000150");
  EXPECT_INT_COLUMN_STATISTICS(table_statistics.column_statistics().at(3), 0.0f, 25, 0, 24);
  EXPECT_FLOAT_COLUMN_STATISTICS(table_statistics.column_statistics().at(5), 0.0f, 150, -986.96f, 9983.38f);
}

TEST_F(GenerateStatisticsTest, GenerateColumnStatisticsUnsampledAndSampled) {
  /** Unsampled statistic generation of lineitem */
  const auto table = load_table("src/test/tables/tpch/sf-0.001/lineitem.tbl");
  const auto table_statistics = generate_table_statistics(*table);

  ASSERT_EQ(table_statistics.column_statistics().size(), 16u);
  EXPECT_EQ(table_statistics.row_count(), 6005u);

  EXPECT_INT_COLUMN_STATISTICS(table_statistics.column_statistics().at(0), 0.0f, 1500, 1, 5988);
  EXPECT_INT_COLUMN_STATISTICS(table_statistics.column_statistics().at(1), 0.0f, 200, 1, 200);
  EXPECT_INT_COLUMN_STATISTICS(table_statistics.column_statistics().at(2), 0.0f, 10, 1, 10);
  EXPECT_INT_COLUMN_STATISTICS(table_statistics.column_statistics().at(3), 0.0f, 7, 1, 7);
  EXPECT_FLOAT_COLUMN_STATISTICS(table_statistics.column_statistics().at(4), 0.0f, 50, 1, 50);
  EXPECT_FLOAT_COLUMN_STATISTICS(table_statistics.column_statistics().at(5), 0.0f, 4525, 901, 55010);
  EXPECT_FLOAT_COLUMN_STATISTICS(table_statistics.column_statistics().at(6), 0.0f, 11, 0, 0.1);
  EXPECT_FLOAT_COLUMN_STATISTICS(table_statistics.column_statistics().at(7), 0.0f, 9, 0, 0.08);
  EXPECT_STRING_COLUMN_STATISTICS(table_statistics.column_statistics().at(8), 0.0f, 3, "A", "R");
  EXPECT_STRING_COLUMN_STATISTICS(table_statistics.column_statistics().at(10), 0.0f, 2266, "1992-01-08", "1998-11-27");
  EXPECT_STRING_COLUMN_STATISTICS(table_statistics.column_statistics().at(15), 0.0f, 5987, " Tiresias alongside of the carefully spec", "zle carefully sauternes. quickly");

  /** Sampled statistic generation of lineitem */
  const auto table_statistics_sampled_0 = generate_table_statistics(*table, 2000);

  ASSERT_EQ(table_statistics_sampled_0.column_statistics().size(), 16u);
  EXPECT_EQ(table_statistics_sampled_0.row_count(), 6005u);

  EXPECT_INT_COLUMN_STATISTICS(table_statistics_sampled_0.column_statistics().at(0), 0.0f, 1500, 1, 5988);
  EXPECT_INT_COLUMN_STATISTICS(table_statistics_sampled_0.column_statistics().at(1), 0.0f, 200, 1, 200);
  EXPECT_INT_COLUMN_STATISTICS(table_statistics_sampled_0.column_statistics().at(2), 0.0f, 10, 1, 10);
  EXPECT_INT_COLUMN_STATISTICS(table_statistics_sampled_0.column_statistics().at(3), 0.0f, 7, 1, 7);
  EXPECT_FLOAT_COLUMN_STATISTICS(table_statistics_sampled_0.column_statistics().at(4), 0.0f, 50, 1, 50);
  EXPECT_FLOAT_COLUMN_STATISTICS(table_statistics_sampled_0.column_statistics().at(5), 0.0f, 4525, 901, 55010);
  EXPECT_FLOAT_COLUMN_STATISTICS(table_statistics_sampled_0.column_statistics().at(6), 0.0f, 11, 0, 0.1);
  EXPECT_FLOAT_COLUMN_STATISTICS(table_statistics_sampled_0.column_statistics().at(7), 0.0f, 9, 0, 0.08);
  EXPECT_STRING_COLUMN_STATISTICS(table_statistics_sampled_0.column_statistics().at(8), 0.0f, 3, "A", "R");
  EXPECT_STRING_COLUMN_STATISTICS(table_statistics_sampled_0.column_statistics().at(10), 0.0f, 2266, "1992-01-08", "1998-11-27");
  EXPECT_STRING_COLUMN_STATISTICS(table_statistics_sampled_0.column_statistics().at(15), 0.0f, 5987, " Tiresias alongside of the carefully spec", "zle carefully sauternes. quickly");
}

#undef EXPECT_INT_COLUMN_STATISTICS
#undef EXPECT_FLOAT_COLUMN_STATISTICS
#undef EXPECT_STRING_COLUMN_STATISTICS

}  // namespace opossum
