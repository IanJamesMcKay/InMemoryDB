#pragma once

#include "abstract_jittable.hpp"
#include "concurrency/transaction_context.hpp"
#include "types.hpp"

namespace opossum {

/* The JitFilter operator filters on a single boolean value and only passes on
 * tuple, for which that value is non-null and true.
 */
template<bool use_ref_pos_list = false>
class JitValidate : public AbstractJittable {
 public:
  JitValidate();

  std::string description() const final;

 protected:
  void _consume(JitRuntimeContext& context) const final;
};

}  // namespace opossum
