#include "../../../base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/equal_height_histogram.hpp"
#include "statistics/chunk_statistics/histograms/abstract_equal_num_elements_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "utils/load_table.hpp"

namespace opossum {

template <typename T>
class BasicHistogramTest : public BaseTest {
  void SetUp() override {
    _int_float4 = load_table("src/test/tables/int_float4.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
};

// TODO(tim): basic tests for float/double/int64
using HistogramTypes =
    ::testing::Types<EqualNumElementsHistogram<int32_t>, EqualWidthHistogram<int32_t>, EqualHeightHistogram<int32_t>>;
TYPED_TEST_CASE(BasicHistogramTest, HistogramTypes);

TYPED_TEST(BasicHistogramTest, CanPruneLowerBound) {
  TypeParam hist(this->_int_float4, 8u);
  hist.generate(ColumnID{0}, 2u);
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0}, PredicateCondition::Equals));
}

TYPED_TEST(BasicHistogramTest, CanPruneUpperBound) {
  TypeParam hist(this->_int_float4, 8u);
  hist.generate(ColumnID{0}, 2u);
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::Equals));
}

TYPED_TEST(BasicHistogramTest, CannotPruneExistingValue) {
  TypeParam hist(this->_int_float4, 8u);
  hist.generate(ColumnID{0}, 2u);
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::Equals));
}

class HistogramTest : public BaseTest {
  void SetUp() override {
    _int_float4 = load_table("src/test/tables/int_float4.tbl");
    _float2 = load_table("src/test/tables/float2.tbl");
    _int_int4 = load_table("src/test/tables/int_int4.tbl");
    _expected_join_result_1 = load_table("src/test/tables/joinoperators/expected_join_result_1.tbl");
    _string2 = load_table("src/test/tables/string2.tbl");
    _string3 = load_table("src/test/tables/string3.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _float2;
  std::shared_ptr<Table> _int_int4;
  std::shared_ptr<Table> _expected_join_result_1;
  std::shared_ptr<Table> _string2;
  std::shared_ptr<Table> _string3;
};

TEST_F(HistogramTest, EqualNumElementsBasic) {
  auto hist = EqualNumElementsHistogram<int32_t>(_int_float4, 8u);
  hist.generate(ColumnID{0}, 2u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0}, PredicateCondition::Equals));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::Equals));
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::Equals));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::Equals));
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::Equals));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::Equals), 2.5f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsUnevenBuckets) {
  auto hist = EqualNumElementsHistogram<int32_t>(_int_float4, 8u);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0}, PredicateCondition::Equals));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::Equals));
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::Equals));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::Equals));
  EXPECT_TRUE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::Equals));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::Equals), 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsFloat) {
  auto hist = EqualNumElementsHistogram<float>(_float2, 8u);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.4f, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.1f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.3f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.2f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.3f, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.5f, PredicateCondition::Equals), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.9f, PredicateCondition::Equals), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::Equals), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.5f, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.1f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.2f, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsString) {
  auto hist = EqualNumElementsHistogram<std::string>(_string2, 8u);
  hist.generate(ColumnID{0}, 4u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("a", PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("aa", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ab", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("b", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("birne", PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("biscuit", PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("bla", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("blubb", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("bums", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("ttt", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("turkey", PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("uuu", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("vvv", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("www", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("xxx", PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("yyy", PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("zzz", PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("zzzzzz", PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsLessThan) {
  auto hist = EqualNumElementsHistogram<int32_t>(_int_float4, 8u);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{70}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12'346}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'457}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::LessThan));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(70, PredicateCondition::LessThan), (70.f - 12) / (123 - 12 + 1) * 2);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::LessThan), 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12'346, PredicateCondition::LessThan), 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::LessThan), 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'457, PredicateCondition::LessThan), 7.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::LessThan), 7.f);
}

TEST_F(HistogramTest, EqualNumElementsFloatLessThan) {
  auto hist = EqualNumElementsHistogram<float>(_float2, 8u);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0.5f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.7f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(2.2f, 2.2f + 1)}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{2.5f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.3f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(3.3f, 3.3f + 1)}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.6f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{5.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}, PredicateCondition::LessThan));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.0f, PredicateCondition::LessThan),
                  (1.0f - 0.5f) / std::nextafter(2.2f - 0.5f, 2.2f - 0.5f + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.7f, PredicateCondition::LessThan),
                  (1.7f - 0.5f) / std::nextafter(2.2f - 0.5f, 2.2f - 0.5f + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(2.2f, 2.2f + 1), PredicateCondition::LessThan), 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.5f, PredicateCondition::LessThan), 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.0f, PredicateCondition::LessThan),
                  4.f + (3.0f - 2.5f) / std::nextafter(3.3f - 2.5f, 3.3f - 2.5f + 1) * 6);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::LessThan),
                  4.f + (3.3f - 2.5f) / std::nextafter(3.3f - 2.5f, 3.3f - 2.5f + 1) * 6);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(3.3f, 3.3f + 1), PredicateCondition::LessThan), 4.f + 6.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::LessThan), 4.f + 6.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::LessThan),
                  4.f + 6.f + (3.9f - 3.6f) / std::nextafter(6.1f - 3.6f, 6.1f - 3.6f + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5.9f, PredicateCondition::LessThan),
                  4.f + 6.f + (5.9f - 3.6f) / std::nextafter(6.1f - 3.6f, 6.1f - 3.6f + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(6.1f, 6.1f + 1), PredicateCondition::LessThan),
                  4.f + 6.f + 4.f);
}

TEST_F(HistogramTest, EqualNumElementsStringLessThan) {
  auto hist = EqualNumElementsHistogram<std::string>(_string3, 4u);
  hist.generate(ColumnID{0}, 4u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality("aaaa", PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("abcd", PredicateCondition::LessThan), 0.f);

  // "abcd"
  constexpr auto bucket_1_lower = 26 * 26 + 2 * 26 + 3;
  // "efgh"
  constexpr auto bucket_1_upper = 4 * 26 * 26 * 26 + 5 * 26 * 26 + 6 * 26 + 7;
  constexpr auto bucket_1_width = (bucket_1_upper - bucket_1_lower + 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("abce", PredicateCondition::LessThan), 1.f / bucket_1_width * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("cccc", PredicateCondition::LessThan),
                  (2 * 26 * 26 * 26 + 2 * 26 * 26 + 2 * 26 + 2 - bucket_1_lower) / bucket_1_width * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("dddd", PredicateCondition::LessThan),
                  (3 * 26 * 26 * 26 + 3 * 26 * 26 + 3 * 26 + 3 - bucket_1_lower) / bucket_1_width * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("efgh", PredicateCondition::LessThan),
                  (4 * 26 * 26 * 26 + 5 * 26 * 26 + 6 * 26 + 7 - bucket_1_lower) / bucket_1_width * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality("efgi", PredicateCondition::LessThan), 4.f);
}

TEST_F(HistogramTest, EqualWidthHistogramBasic) {
  auto hist = EqualWidthHistogram<int32_t>(_int_int4, 8u);
  hist.generate(ColumnID{1}, 6u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(7, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(10, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(11, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(13, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(14, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(15, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(17, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(18, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualWidthHistogramUnevenBuckets) {
  auto hist = EqualWidthHistogram<int32_t>(_int_int4, 8u);
  hist.generate(ColumnID{1}, 4u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(7, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(9, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(10, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(11, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(13, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(14, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(15, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(17, PredicateCondition::Equals), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(18, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualWidthFloat) {
  auto hist = EqualWidthHistogram<float>(_float2, 8u);
  hist.generate(ColumnID{0}, 4u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.4f, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.1f, PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.3f, PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.9f, PredicateCondition::Equals), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.0f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.2f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.3f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.5f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.9f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.1f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.2f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::Equals), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.4f, PredicateCondition::Equals), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::Equals), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::Equals), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4.4f, PredicateCondition::Equals), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4.5f, PredicateCondition::Equals), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.1f, PredicateCondition::Equals), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.2f, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualWidthLessThan) {
  auto hist = EqualWidthHistogram<int32_t>(_int_float4, 8u);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{70}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12'346}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'457}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::LessThan));

  // The first bucket's range is one value wider (because (123'456 - 12 + 1) % 3 = 1).
  const auto bucket_width = (123'456 - 12 + 1) / 3;

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(70, PredicateCondition::LessThan), (70.f - 12) / (bucket_width + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::LessThan),
                  (1'234.f - 12) / (bucket_width + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12'346, PredicateCondition::LessThan),
                  (12'346.f - 12) / (bucket_width + 1) * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(80'000, PredicateCondition::LessThan), 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::LessThan),
                  4.f + (123'456.f - (12 + 2 * bucket_width + 1)) / bucket_width * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'457, PredicateCondition::LessThan), 4.f + 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::LessThan), 4.f + 3.f);
}

TEST_F(HistogramTest, EqualWidthFloatLessThan) {
  auto hist = EqualWidthHistogram<float>(_float2, 8u);
  hist.generate(ColumnID{0}, 3u);

  const auto bucket_width = std::nextafter(6.1f - 0.5f, 6.1f - 0.5f + 1) / 3;

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0.5f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.7f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(0.5f + bucket_width, 0.5f + bucket_width + 1)},
                              PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{2.5f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.3f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.6f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(0.5f + 2 * bucket_width, 0.5f + 2 * bucket_width + 1)},
                              PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{4.4f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{5.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}, PredicateCondition::LessThan));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.0f, PredicateCondition::LessThan), (1.0f - 0.5f) / bucket_width * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.7f, PredicateCondition::LessThan), (1.7f - 0.5f) / bucket_width * 4);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(0.5f + bucket_width, 0.5f + bucket_width + 1),
                                            PredicateCondition::LessThan),
                  4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.5f, PredicateCondition::LessThan),
                  4.f + (2.5f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.0f, PredicateCondition::LessThan),
                  4.f + (3.0f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::LessThan),
                  4.f + (3.3f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::LessThan),
                  4.f + (3.6f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::LessThan),
                  4.f + (3.9f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(0.5f + 2 * bucket_width, 0.5f + 2 * bucket_width + 1),
                                            PredicateCondition::LessThan),
                  4.f + 7.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4.4f, PredicateCondition::LessThan),
                  4.f + 7.f + (4.4f - (0.5f + 2 * bucket_width)) / bucket_width * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5.9f, PredicateCondition::LessThan),
                  4.f + 7.f + (5.9f - (0.5f + 2 * bucket_width)) / bucket_width * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(6.1f, 6.1f + 1), PredicateCondition::LessThan),
                  4.f + 7.f + 3.f);
}

TEST_F(HistogramTest, EqualHeightHistogramBasic) {
  auto hist = EqualHeightHistogram<int32_t>(_expected_join_result_1, 8u);
  hist.generate(ColumnID{1}, 4u);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2, PredicateCondition::Equals), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6, PredicateCondition::Equals), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(8, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(9, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(10, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(18, PredicateCondition::Equals), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(20, PredicateCondition::Equals), 6 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(21, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualHeightHistogramUnevenBuckets) {
  auto hist = EqualHeightHistogram<int32_t>(_expected_join_result_1, 8u);
  hist.generate(ColumnID{1}, 5u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1, PredicateCondition::Equals), 5 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(7, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(8, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(9, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(10, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::Equals), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(18, PredicateCondition::Equals), 5 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(19, PredicateCondition::Equals), 5 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(20, PredicateCondition::Equals), 5 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(21, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualHeightFloat) {
  auto hist = EqualHeightHistogram<float>(_float2, 8u);
  hist.generate(ColumnID{0}, 4u);

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.4f, PredicateCondition::Equals), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.1f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.3f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.2f, PredicateCondition::Equals), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.3f, PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.5f, PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.9f, PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.1f, PredicateCondition::Equals), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.2f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.5f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4.4f, PredicateCondition::Equals), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(4.5f, PredicateCondition::Equals), 4 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.1f, PredicateCondition::Equals), 4 / 1.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(6.2f, PredicateCondition::Equals), 0.f);
}

TEST_F(HistogramTest, EqualHeightLessThan) {
  auto hist = EqualHeightHistogram<int32_t>(_int_float4, 8u);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{12}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{70}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'234}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{12'346}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'456}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{123'457}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1'000'000}, PredicateCondition::LessThan));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(70, PredicateCondition::LessThan), (70.f - 12) / (12'345 - 12 + 1) * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'234, PredicateCondition::LessThan),
                  (1'234.f - 12) / (12'345 - 12 + 1) * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(12'346, PredicateCondition::LessThan), 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(80'000, PredicateCondition::LessThan),
                  3.f + (80'000.f - 12'346) / (123'456 - 12'346 + 1) * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'456, PredicateCondition::LessThan),
                  3.f + (123'456.f - 12'346) / (123'456 - 12'346 + 1) * 3);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(123'457, PredicateCondition::LessThan), 3.f + 3.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1'000'000, PredicateCondition::LessThan), 3.f + 3.f);
}

TEST_F(HistogramTest, EqualHeightFloatLessThan) {
  auto hist = EqualHeightHistogram<float>(_float2, 8u);
  hist.generate(ColumnID{0}, 3u);

  EXPECT_TRUE(hist.can_prune(AllTypeVariant{0.5f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{1.7f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{2.2f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(2.5f, 2.5f + 1)}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.0f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.3f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(3.3f, 3.3f + 1)}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.6f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{3.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(4.4f, 4.4f + 1)}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{5.9f}, PredicateCondition::LessThan));
  EXPECT_FALSE(hist.can_prune(AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}, PredicateCondition::LessThan));

  EXPECT_FLOAT_EQ(hist.estimate_cardinality(0.5f, PredicateCondition::LessThan), 0.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.0f, PredicateCondition::LessThan), (1.0f - 0.5f) / (2.5f - 0.5f) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(1.7f, PredicateCondition::LessThan), (1.7f - 0.5f) / (2.5f - 0.5f) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(2.2f, PredicateCondition::LessThan), (2.2f - 0.5f) / (2.5f - 0.5f) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(2.5f, 2.5f + 1), PredicateCondition::LessThan), 5.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.0f, PredicateCondition::LessThan),
                  5.f + (3.0f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.3f, PredicateCondition::LessThan),
                  5.f + (3.3f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.6f, PredicateCondition::LessThan),
                  5.f + (3.6f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(3.9f, PredicateCondition::LessThan),
                  5.f + (3.9f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(4.4f, 4.4f + 1), PredicateCondition::LessThan), 5.f + 5.f);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(5.9f, PredicateCondition::LessThan),
                  5.f + 5.f + (5.9f - (std::nextafter(4.4f, 4.4f + 1))) / (6.1f - std::nextafter(4.4f, 4.4f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist.estimate_cardinality(std::nextafter(6.1f, 6.1f + 1), PredicateCondition::LessThan),
                  5.f + 5.f + 5.f);
}

TEST_F(HistogramTest, StringPrefixSize) {
  EXPECT_NO_THROW(EqualNumElementsHistogram<std::string>(_string2, 13u));
  EXPECT_THROW(EqualNumElementsHistogram<std::string>(_string2, 14u), std::exception);
}

TEST_F(HistogramTest, EstimateCardinalityUnsupportedCharacters) {
  auto hist = EqualNumElementsHistogram<std::string>(_string2, 4u);
  hist.generate(ColumnID{0}, 4u);

  EXPECT_NO_THROW(hist.estimate_cardinality("abcd", PredicateCondition::Equals));
  EXPECT_NO_THROW(hist.estimate_cardinality("abcd", PredicateCondition::Equals));
  EXPECT_THROW(hist.estimate_cardinality("abc1", PredicateCondition::Equals), std::exception);
  EXPECT_THROW(hist.estimate_cardinality("abc1", PredicateCondition::Equals), std::exception);
  EXPECT_THROW(hist.estimate_cardinality("Abc", PredicateCondition::Equals), std::exception);
  EXPECT_THROW(hist.estimate_cardinality("Abc", PredicateCondition::Equals), std::exception);
  EXPECT_THROW(hist.estimate_cardinality("@", PredicateCondition::Equals), std::exception);
  EXPECT_THROW(hist.estimate_cardinality("@", PredicateCondition::Equals), std::exception);
}

class HistogramPrivateTest : public BaseTest {
  void SetUp() override {
    const auto _string2 = load_table("src/test/tables/string2.tbl");
    _hist = std::make_shared<EqualNumElementsHistogram<std::string>>(_string2, 4u);
    _hist->generate(ColumnID{0}, 2u);
  }

 protected:
  std::string previous_value(const std::string& value) {
    return _hist->_previous_value(value);
  }

  std::string next_value(const std::string& value) {
    return _hist->_next_value(value);
  }

  uint64_t convert_string_to_number_representation(const std::string& value) {
    return _hist->_convert_string_to_number_representation(value);
  }

 protected:
  std::shared_ptr<EqualNumElementsHistogram<std::string>> _hist;
};

TEST_F(HistogramPrivateTest, PreviousValueString) {
  EXPECT_EQ(previous_value(""), "");
  EXPECT_EQ(previous_value("a"), "");
  EXPECT_EQ(previous_value("aaa"), "aa");
  EXPECT_EQ(previous_value("abcd"), "abcc");
  EXPECT_EQ(previous_value("abzz"), "abzy");
  EXPECT_EQ(previous_value("abca"), "abc");
  EXPECT_EQ(previous_value("abaa"), "aba");
  EXPECT_EQ(previous_value("aba"), "ab");
}

TEST_F(HistogramPrivateTest, NextValueString) {
  EXPECT_EQ(next_value(""), "a");
  EXPECT_EQ(next_value("abcd"), "abce");
  EXPECT_EQ(next_value("abaz"), "abba");
  EXPECT_EQ(next_value("abzz"), "acaa");
  EXPECT_EQ(next_value("abca"), "abcb");
  EXPECT_EQ(next_value("abaa"), "abab");
  EXPECT_EQ(next_value("zzzz"), "zzzza");
}

TEST_F(HistogramPrivateTest, StringToNumber) {
  EXPECT_EQ(convert_string_to_number_representation("aaaa"), 0ul);
  EXPECT_EQ(convert_string_to_number_representation("aaab"), 1ul);
  EXPECT_EQ(convert_string_to_number_representation("bhja"),
            26ul * 26ul * 26ul + 7ul * 26ul * 26ul + 9ul * 26ul);
  EXPECT_EQ(convert_string_to_number_representation("zzzz"), 26ul * 26ul * 26ul * 26ul - 1ul);

  EXPECT_EQ(convert_string_to_number_representation("aaaa"), convert_string_to_number_representation("a"));
  EXPECT_EQ(convert_string_to_number_representation("dcba"), convert_string_to_number_representation("dcb"));
  EXPECT_NE(convert_string_to_number_representation("abcd"), convert_string_to_number_representation("bcd"));
}

// TODO(tim): remove
TEST_F(HistogramTest, StringComparisonTest) {
  EXPECT_TRUE(std::string("abcd") < std::string("abce"));
  EXPECT_TRUE(std::string("abc") < std::string("abca"));

  EXPECT_TRUE(std::string("Z") < std::string("a"));
  EXPECT_FALSE(std::string("azaaaaaaa") < std::string("aza"));
  EXPECT_TRUE(std::string("aZaaaaaaa") < std::string("aza"));
}

}  // namespace opossum
