#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/equal_height_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_num_elements_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "utils/load_table.hpp"

namespace opossum {

template <typename T>
class BasicHistogramTest : public BaseTest {
  void SetUp() override { _int_float4 = load_table("src/test/tables/int_float4.tbl"); }

 protected:
  std::shared_ptr<Table> _int_float4;
};

using HistogramTypes =
    ::testing::Types<EqualNumElementsHistogram<int32_t>, EqualWidthHistogram<int32_t>, EqualHeightHistogram<int32_t>>;
TYPED_TEST_CASE(BasicHistogramTest, HistogramTypes);

TYPED_TEST(BasicHistogramTest, CanPruneLowerBound) {
  TypeParam hist(this->_int_float4);
  hist.generate(ColumnID{0}, 2u);
  ASSERT_TRUE(hist.can_prune(AllTypeVariant{0}, PredicateCondition::Equals));
}

TYPED_TEST(BasicHistogramTest, CanPruneUpperBound) {
  TypeParam hist(this->_int_float4);
  hist.generate(ColumnID{0}, 2u);
  ASSERT_TRUE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::Equals));
}

TYPED_TEST(BasicHistogramTest, CannotPruneExistingValue) {
  TypeParam hist(this->_int_float4);
  hist.generate(ColumnID{0}, 2u);
  ASSERT_FALSE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::Equals));
}

class HistogramTest : public ::testing::Test {
  void SetUp() override {
    _int_float4 = load_table("src/test/tables/int_float4.tbl");
    _float2 = load_table("src/test/tables/float2.tbl");
    _int_int4 = load_table("src/test/tables/int_int4.tbl");
    _expected_join_result_1 = load_table("src/test/tables/joinoperators/expected_join_result_1.tbl");
    _string2 = load_table("src/test/tables/string2.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _float2;
  std::shared_ptr<Table> _int_int4;
  std::shared_ptr<Table> _expected_join_result_1;
  std::shared_ptr<Table> _string2;
};

TEST_F(HistogramTest, EqualNumElementsBasic) {
  auto hist = EqualNumElementsHistogram<int32_t>(_int_float4);
  hist.generate(ColumnID{0}, 2u);
  EXPECT_EQ(hist.num_buckets(), 2u);
  EXPECT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0}, PredicateCondition::Equals));
  EXPECT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 1.f);
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::Equals));
  EXPECT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::Equals), 0.f);
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::Equals));
  EXPECT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::Equals), 2.5f);
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::Equals));
  EXPECT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::Equals), 0.f);
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::Equals));
}

TEST_F(HistogramTest, EqualNumElementsUnevenBuckets) {
  auto hist = EqualNumElementsHistogram<int32_t>(_int_float4);
  hist.generate(ColumnID{0}, 3u);
  EXPECT_EQ(hist.num_buckets(), 3u);
  EXPECT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0}, PredicateCondition::Equals));
  EXPECT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 1.f);
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::Equals));
  EXPECT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::Equals), 0.f);
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::Equals));
  EXPECT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::Equals), 3.f);
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::Equals));
  EXPECT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::Equals), 0.f);
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::Equals));
}

TEST_F(HistogramTest, EqualNumElementsFloat) {
  auto hist = EqualNumElementsHistogram<float>(_float2);
  hist.generate(ColumnID{0}, 3u);
  EXPECT_EQ(hist.num_buckets(), 3u);
  EXPECT_EQ(hist.estimate_cardinality(0.4, PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality(0.5, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(1.1, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(1.3, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(2.2, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(2.3, PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality(2.5, PredicateCondition::Equals), 6 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(2.9, PredicateCondition::Equals), 6 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(3.3, PredicateCondition::Equals), 6 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(3.5, PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality(3.6, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(3.9, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(6.1, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(6.2, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsString) {
  auto hist = EqualNumElementsHistogram<std::string>(_string2);
  hist.generate(ColumnID{0}, 4u);
  EXPECT_EQ(hist.num_buckets(), 4u);
  EXPECT_EQ(hist.estimate_cardinality("1", PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality("12v", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("13", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("b", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("birne", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("biscuit", PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality("bla", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("blubb", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("bums", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("ttt", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("turkey", PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality("uuu", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("vvv", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("www", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("xxx", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality("yyy", PredicateCondition::Equals), 4 / 2.f);
  EXPECT_EQ(hist.estimate_cardinality("zzz", PredicateCondition::Equals), 4 / 2.f);
  EXPECT_EQ(hist.estimate_cardinality("zzzzzz", PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualWidthHistogramBasic) {
  auto hist = EqualWidthHistogram<int32_t>(_int_int4);
  hist.generate(ColumnID{1}, 6u);
  EXPECT_EQ(hist.num_buckets(), 6u);
  EXPECT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 1 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(13, PredicateCondition::Equals), 2 / 3.f);
  EXPECT_EQ(hist.estimate_cardinality(20, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualWidthHistogramUnevenBuckets) {
  auto hist = EqualWidthHistogram<int32_t>(_int_int4);
  hist.generate(ColumnID{1}, 4u);
  EXPECT_EQ(hist.num_buckets(), 4u);
  EXPECT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 6 / 5.f);
  EXPECT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 6 / 5.f);
  EXPECT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 1 / 5.f);
  EXPECT_EQ(hist.estimate_cardinality(7, PredicateCondition::Equals), 1 / 5.f);
  EXPECT_EQ(hist.estimate_cardinality(10, PredicateCondition::Equals), 2 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(13, PredicateCondition::Equals), 2 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(14, PredicateCondition::Equals), 2 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(17, PredicateCondition::Equals), 2 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(20, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualHeightHistogramBasic) {
  auto hist = EqualHeightHistogram<int32_t>(_expected_join_result_1);
  hist.generate(ColumnID{1}, 4u);
  EXPECT_EQ(hist.num_buckets(), 4u);
  EXPECT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 6 / 2.f);
  EXPECT_EQ(hist.estimate_cardinality(2, PredicateCondition::Equals), 6 / 2.f);
  EXPECT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 6 / 5.f);
  EXPECT_EQ(hist.estimate_cardinality(6, PredicateCondition::Equals), 6 / 5.f);
  EXPECT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 6 / 11.f);
  EXPECT_EQ(hist.estimate_cardinality(20, PredicateCondition::Equals), 6 / 2.f);
  EXPECT_EQ(hist.estimate_cardinality(21, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualHeightHistogramUnevenBuckets) {
  auto hist = EqualHeightHistogram<int32_t>(_expected_join_result_1);
  hist.generate(ColumnID{1}, 5u);
  EXPECT_EQ(hist.num_buckets(), 5u);
  EXPECT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 4 / 1.f);
  EXPECT_EQ(hist.estimate_cardinality(2, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_EQ(hist.estimate_cardinality(7, PredicateCondition::Equals), 4 / 2.f);
  EXPECT_EQ(hist.estimate_cardinality(9, PredicateCondition::Equals), 4 / 2.f);
  EXPECT_EQ(hist.estimate_cardinality(10, PredicateCondition::Equals), 4 / 11.f);
  EXPECT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 4 / 11.f);
  EXPECT_EQ(hist.estimate_cardinality(20, PredicateCondition::Equals), 4 / 11.f);
  EXPECT_EQ(hist.estimate_cardinality(21, PredicateCondition::Equals), 0.f);
}

}  // namespace opossum
