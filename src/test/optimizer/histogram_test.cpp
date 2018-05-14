#include "gtest/gtest.h"

#include "optimizer/histograms/equal_height_histogram.hpp"
#include "optimizer/histograms/equal_num_elements_histogram.hpp"
#include "optimizer/histograms/equal_width_histogram.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class HistogramTest : public ::testing::Test {
  void SetUp() override {
    _int_float4 = load_table("src/test/tables/int_float4.tbl");
    _int_int4 = load_table("src/test/tables/int_int4.tbl");
    _expected_join_result_1 = load_table("src/test/tables/joinoperators/expected_join_result_1.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _int_int4;
  std::shared_ptr<Table> _expected_join_result_1;
};

TEST_F(HistogramTest, BasicEqualNumElementsHistogramTest) {
  auto hist = EqualNumElementsHistogram<int32_t>(_int_float4);
  hist.generate(ColumnID{0}, 2u);
  EXPECT_EQ(hist.num_buckets(), 2u);
  EXPECT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 1.f);
  EXPECT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::Equals), 2.5f);
  EXPECT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, BasicEqualWidthHistogramTest) {
  auto hist = EqualWidthHistogram<int32_t>(_int_int4);
  hist.generate(ColumnID{1}, 6u);
  EXPECT_EQ(hist.num_buckets(), 6u);
  EXPECT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 1 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(13, PredicateCondition::Equals), 2 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(20, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, BasicEqualHeightHistogramTest) {
  auto hist = EqualHeightHistogram<int32_t>(_expected_join_result_1);
  hist.generate(ColumnID{1}, 4u);
  EXPECT_EQ(hist.num_buckets(), 4u);
  EXPECT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(2, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 6 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 6 / 12.f);
  EXPECT_EQ(hist.estimate_cardinality(20, PredicateCondition::Equals), 6.f);
  EXPECT_EQ(hist.estimate_cardinality(21, PredicateCondition::Equals), 0.f);
}

}  // namespace opossum
