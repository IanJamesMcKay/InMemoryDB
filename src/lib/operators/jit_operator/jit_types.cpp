#include "jit_types.hpp"

namespace opossum {

#define JIT_VARIANT_VECTOR_GET(r, d, type)          \
  template <>                                       \
  BOOST_PP_TUPLE_ELEM(3, 0, type)                   \
  JitVariantVector::get(const size_t index) {       \
    return BOOST_PP_TUPLE_ELEM(3, 1, type)[index];  \
  }

#define JIT_VARIANT_VECTOR_SET(r, d, type)                                                      \
  template <>                                                                                   \
  void JitVariantVector::set(const size_t index, const BOOST_PP_TUPLE_ELEM(3, 0, type) value) { \
    BOOST_PP_TUPLE_ELEM(3, 1, type)[index] = value;                                             \
  }

#define JIT_VARIANT_VECTOR_GROW(r, d, type)                                       \
  template <>                                                                     \
  size_t JitVariantVector::grow<BOOST_PP_TUPLE_ELEM(3, 0, type)>() {              \
    BOOST_PP_TUPLE_ELEM(3, 1, type).push_back(BOOST_PP_TUPLE_ELEM(3, 0, type)()); \
    return BOOST_PP_TUPLE_ELEM(3, 1, type).size() - 1;                            \
  }

// Generate get and set methods for all data types defined in the JIT_DATA_TYPE_INFO
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_GET, _, JIT_DATA_TYPE_INFO)
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_SET, _, JIT_DATA_TYPE_INFO)
//BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_GROW, _, JIT_DATA_TYPE_INFO)

/*size_t JitHashmapValue::grow(JitRuntimeContext& ctx) const {
  switch (_data_type) {
    case DataType::Bool:
      return ctx.hashmap.values[_vector_index].grow<bool>();
    case DataType::Int:
      return ctx.hashmap.values[_vector_index].grow<int32_t>();
    case DataType::Long:
      return ctx.hashmap.values[_vector_index].grow<int64_t>();
    case DataType::Float:
      return ctx.hashmap.values[_vector_index].grow<float>();
    case DataType::Double:
      return ctx.hashmap.values[_vector_index].grow<double>();
    case DataType::String:
      return ctx.hashmap.values[_vector_index].grow<std::string>();
    case DataType::Null:
      Fail("unreachable");
  }
}*/

// cleanup
#undef JIT_VARIANT_VECTOR_GET
#undef JIT_VARIANT_VECTOR_SET

}  // namespace opossum
