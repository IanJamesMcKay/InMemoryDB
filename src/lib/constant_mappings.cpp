#include "constant_mappings.hpp"

#include <boost/bimap.hpp>
#include <boost/hana/fold.hpp>

#include <string>
#include <unordered_map>

#include "sql/Expr.h"
#include "sql/SelectStatement.h"

#include "storage/encoding_type.hpp"
#include "storage/vector_compression/vector_compression.hpp"

namespace opossum {

/*
 * boost::bimap does not support initializer_lists.
 * Instead we use this helper function to have an initializer_list-friendly interface.
 */
template<typename L, typename R>
boost::bimap<L, R> make_bimap(std::initializer_list<typename boost::bimap<L, R>::value_type> list) {
  return boost::bimap<L, R>(list.begin(), list.end());
}

const boost::bimap<PredicateCondition, std::string> predicate_condition_to_string =
make_bimap<PredicateCondition, std::string>({
                                            {PredicateCondition::Equals,            "="},
                                            {PredicateCondition::NotEquals,         "!="},
                                            {PredicateCondition::LessThan,          "<"},
                                            {PredicateCondition::LessThanEquals,    "<="},
                                            {PredicateCondition::GreaterThan,       ">"},
                                            {PredicateCondition::GreaterThanEquals, ">="},
                                            {PredicateCondition::Between,           "BETWEEN"},
                                            {PredicateCondition::Like,              "LIKE"},
                                            {PredicateCondition::NotLike,           "NOT LIKE"},
                                            {PredicateCondition::IsNull,            "IS NULL"},
                                            {PredicateCondition::IsNotNull,         "IS NOT NULL"},
                                            });

const boost::bimap<OperatorType, std::string> operator_type_to_string =
make_bimap<OperatorType, std::string>({
                                            {OperatorType::Aggregate,      "Aggregate"},
                                            {OperatorType::Delete,         "Delete"},
                                            {OperatorType::Difference,     "Difference"},
                                            {OperatorType::ExportBinary,   "ExportBinary"},
                                            {OperatorType::ExportCsv,      "ExportCsv"},
                                            {OperatorType::GetTable,       "GetTable"},
                                            {OperatorType::ImportBinary,   "ImportBinary"},
                                            {OperatorType::ImportCsv,      "ImportCsv"},
                                            {OperatorType::IndexScan,      "IndexScan"},
                                            {OperatorType::Insert,         "Insert"},
                                            {OperatorType::JoinHash,       "JoinHash"},
                                            {OperatorType::JoinIndex,      "JoinIndex"},
                                            {OperatorType::JoinNestedLoop, "JoinNestedLoop"},
                                            {OperatorType::JoinSortMerge,  "JoinSortMerge"},
                                            {OperatorType::Limit,          "Limit"},
                                            {OperatorType::Print,          "Print"},
                                            {OperatorType::Product,        "Product"},
                                            {OperatorType::Projection,     "Projection"},
                                            {OperatorType::Sort,           "Sort"},
                                            {OperatorType::TableScan,      "TableScan"},
                                            {OperatorType::TableWrapper,   "TableWrapper"},
                                            {OperatorType::UnionAll,       "UnionAll"},
                                            {OperatorType::UnionPositions, "UnionPositions"},
                                            {OperatorType::Update,         "Update"},
                                            {OperatorType::Validate,       "Validate"},
                                            {OperatorType::CreateView,     "CreateView"},
                                            {OperatorType::DropView,       "DropView"},
                                            {OperatorType::ShowColumns,    "ShowColumns"},
                                            {OperatorType::ShowTables,     "ShowTables"},
                                            {OperatorType::Mock,           "Mock"}
                                            });

const boost::bimap<CostFeature, std::string> cost_feature_to_string =
make_bimap<CostFeature, std::string>({
                                            {CostFeature::LeftInputRowCount, "LeftInputRowCount"},
                                            {CostFeature::RightInputRowCount, "RightInputRowCount"},
                                            {CostFeature::InputRowCountProduct, "InputRowCountProduct"},
                                            {CostFeature::LeftInputReferenceRowCount, "LeftInputReferenceRowCount"},
                                            {CostFeature::RightInputReferenceRowCount, "RightInputReferenceRowCount"},
                                            {CostFeature::LeftInputRowCountLogN, "LeftInputRowCountLogN"},
                                            {CostFeature::RightInputRowCountLogN, "RightInputRowCountLogN"},
                                            {CostFeature::MajorInputRowCount, "MajorInputRowCount"},
                                            {CostFeature::MinorInputRowCount, "MinorInputRowCount"},
                                            {CostFeature::MajorInputReferenceRowCount, "MajorInputReferenceRowCount"},
                                            {CostFeature::MinorInputReferenceRowCount, "MinorInputReferenceRowCount"},
                                            {CostFeature::OutputRowCount, "OutputRowCount"},
                                            {CostFeature::OutputDereferenceRowCount, "OutputDereferenceRowCount"},
                                            {CostFeature::OperatorType, "OperatorType"},
                                            {CostFeature::LeftDataType, "LeftDataType"},
                                            {CostFeature::RightDataType, "RightDataType"},
                                            {CostFeature::PredicateCondition, "PredicateCondition"},
                                            {CostFeature::LeftInputIsReferences, "LeftInputIsReferences"},
                                            {CostFeature::RightInputIsReferences, "RightInputIsReferences"},
                                            {CostFeature::RightOperandIsColumn, "RightOperandIsColumn"},
                                            {CostFeature::LeftInputIsMajor, "LeftInputIsMajor"},
                                            });

const std::unordered_map<PredicateCondition, ExpressionType> predicate_condition_to_expression_type = {
{PredicateCondition::Equals,            ExpressionType::Equals},
{PredicateCondition::NotEquals,         ExpressionType::NotEquals},
{PredicateCondition::LessThan,          ExpressionType::LessThan},
{PredicateCondition::LessThanEquals,    ExpressionType::LessThanEquals},
{PredicateCondition::GreaterThan,       ExpressionType::GreaterThan},
{PredicateCondition::GreaterThanEquals, ExpressionType::GreaterThanEquals},
{PredicateCondition::Between,           ExpressionType::Between},
{PredicateCondition::Like,              ExpressionType::Like},
{PredicateCondition::NotLike,           ExpressionType::NotLike},
{PredicateCondition::IsNull,            ExpressionType::IsNull},
{PredicateCondition::IsNotNull,         ExpressionType::IsNotNull}};

const std::unordered_map<ExpressionType, std::string> expression_type_to_string = {
{ExpressionType::Literal,           "Literal"},
{ExpressionType::Star,              "Star"},
{ExpressionType::Placeholder,       "Parameter"},
{ExpressionType::Column,            "Column"},
{ExpressionType::Function,          "Function"},
{ExpressionType::Select,            "Select"},
/*Arithmetic operators*/
{ExpressionType::Addition,          "Addition"},
{ExpressionType::Subtraction,       "Subtraction"},
{ExpressionType::Multiplication,    "Multiplication"},
{ExpressionType::Division,          "Division"},
{ExpressionType::Modulo,            "Modulo"},
{ExpressionType::Power,             "Power"},
/*Logical operators*/
{ExpressionType::Equals,            "Equals"},
{ExpressionType::NotEquals,         "NotEquals"},
{ExpressionType::LessThan,          "LessThan"},
{ExpressionType::LessThanEquals,    "LessThanEquals"},
{ExpressionType::GreaterThan,       "GreaterThan"},
{ExpressionType::GreaterThanEquals, "GreaterThanEquals"},
{ExpressionType::Like,              "Like"},
{ExpressionType::NotLike,           "NotLike"},
{ExpressionType::And,               "And"},
{ExpressionType::Or,                "Or"},
{ExpressionType::Between,           "Between"},
{ExpressionType::Not,               "Not"},
/*Set operators*/
{ExpressionType::In,                "In"},
{ExpressionType::Exists,            "Exists"},
/*Other*/
{ExpressionType::IsNull,            "IsNull"},
{ExpressionType::IsNotNull,         "IsNotNull"},
{ExpressionType::Case,              "Case"},
{ExpressionType::Hint,              "Hint"},
};

const std::unordered_map<OrderByMode, std::string> order_by_mode_to_string = {
{OrderByMode::Ascending,  "Ascending"},
{OrderByMode::Descending, "Descending"},
};

const std::unordered_map<hsql::OperatorType, ExpressionType> operator_type_to_expression_type = {
{hsql::kOpPlus,       ExpressionType::Addition},
{hsql::kOpMinus,      ExpressionType::Subtraction},
{hsql::kOpAsterisk,   ExpressionType::Multiplication},
{hsql::kOpSlash,      ExpressionType::Division},
{hsql::kOpPercentage, ExpressionType::Modulo},
{hsql::kOpCaret,      ExpressionType::Power},
{hsql::kOpBetween,    ExpressionType::Between},
{hsql::kOpEquals,     ExpressionType::Equals},
{hsql::kOpNotEquals,  ExpressionType::NotEquals},
{hsql::kOpLess,       ExpressionType::LessThan},
{hsql::kOpLessEq,     ExpressionType::LessThanEquals},
{hsql::kOpGreater,    ExpressionType::GreaterThan},
{hsql::kOpGreaterEq,  ExpressionType::GreaterThanEquals},
{hsql::kOpLike,       ExpressionType::Like},
{hsql::kOpNotLike,    ExpressionType::NotLike},
{hsql::kOpCase,       ExpressionType::Case},
{hsql::kOpExists,     ExpressionType::Exists},
{hsql::kOpIn,         ExpressionType::In},
{hsql::kOpIsNull,     ExpressionType::IsNull},
{hsql::kOpOr,         ExpressionType::Or},
};

const std::unordered_map<hsql::OrderType, OrderByMode> order_type_to_order_by_mode = {
{hsql::kOrderAsc,  OrderByMode::Ascending},
{hsql::kOrderDesc, OrderByMode::Descending},
};

const std::unordered_map<ExpressionType, std::string> expression_type_to_operator_string = {
{ExpressionType::Addition,          "+"},
{ExpressionType::Subtraction,       "-"},
{ExpressionType::Multiplication,    "*"},
{ExpressionType::Division,          "/"},
{ExpressionType::Modulo,            "%"},
{ExpressionType::Power,             "^"},
{ExpressionType::Equals,            "="},
{ExpressionType::NotEquals,         "!="},
{ExpressionType::LessThan,          "<"},
{ExpressionType::LessThanEquals,    "<="},
{ExpressionType::GreaterThan,       ">"},
{ExpressionType::GreaterThanEquals, ">="},
{ExpressionType::Like,              "LIKE"},
{ExpressionType::NotLike,           "NOT LIKE"},
{ExpressionType::And,               "AND"},
{ExpressionType::Or,                "OR"},
{ExpressionType::Between,           "BETWEEN"},
{ExpressionType::Not,               "NOT"},
};

const std::unordered_map<JoinMode, std::string> join_mode_to_string = {
{JoinMode::Cross,   "Cross"},
{JoinMode::Inner,   "Inner"},
{JoinMode::Left,    "Left"},
{JoinMode::Natural, "Natural"},
{JoinMode::Outer,   "Outer"},
{JoinMode::Right,   "Right"},
{JoinMode::Self,    "Self"},
{JoinMode::Semi,    "Semi"},
{JoinMode::Anti,    "Anti"},
};

const std::unordered_map<UnionMode, std::string> union_mode_to_string = {{UnionMode::Positions, "UnionPositions"}};

const boost::bimap<AggregateFunction, std::string> aggregate_function_to_string =
make_bimap<AggregateFunction, std::string>({
                                           {AggregateFunction::Min,           "MIN"},
                                           {AggregateFunction::Max,           "MAX"},
                                           {AggregateFunction::Sum,           "SUM"},
                                           {AggregateFunction::Avg,           "AVG"},
                                           {AggregateFunction::Count,         "COUNT"},
                                           {AggregateFunction::CountDistinct, "COUNT DISTINCT"},
                                           });

const boost::bimap<DataType, std::string> data_type_to_string =
make_bimap<DataType, std::string>({
                                           {DataType::Int,           "int"},
                                           {DataType::Long,           "long"},
                                           {DataType::Float,           "float"},
                                           {DataType::Double,           "double"},
                                           {DataType::String,         "string"},
                                           {DataType::Null, "null"},
                                           });


const std::unordered_map<EncodingType, std::string> encoding_type_to_string = {
{EncodingType::DeprecatedDictionary, "Dictionary (Deprecated)"},
{EncodingType::Dictionary,           "Dictionary"},
{EncodingType::RunLength,            "RunLength"},
};

const std::unordered_map<VectorCompressionType, std::string> vector_compression_type_to_string = {
{VectorCompressionType::FixedSizeByteAligned, "Fixed-size byte-aligned"},
{VectorCompressionType::SimdBp128,            "SIMD-BP128"},
};
const boost::bimap<TableType, std::string> table_type_to_string =
make_bimap<TableType, std::string>({{TableType::Data,       "Data"},
                                    {TableType::References, "References"}});


}  // namespace opossum
