#include "gtest/gtest.h"

#include "optimizer/histograms/equal_num_elements_histogram.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class HistogramTest : public ::testing::Test {};

TEST_F(HistogramTest, BasicTest) {
  const auto table = load_table("src/test/tables/int_float4.tbl");
  auto hist = EqualNumElementsHistogram<int32_t>(table);
  hist.generate(ColumnID{0}, 2u);
  EXPECT_EQ(hist.num_buckets(), 2u);
  EXPECT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 1.f);
  EXPECT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::Equals), 2.5f);
  EXPECT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::Equals), 0.f);
}

}  // namespace opossum
