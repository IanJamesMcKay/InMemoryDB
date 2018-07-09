#pragma once

#include "abstract_jittable.hpp"
#include "concurrency/transaction_context.hpp"
#include "types.hpp"

namespace opossum {

/* The JitFilter operator filters on a single boolean value and only passes on
 * tuple, for which that value is non-null and true.
 */
class JitValidate : public AbstractJittable {
 public:
  explicit JitValidate(const bool use_ref_pos_list = false);

  std::string description() const final;

  void set_use_ref_pos_list(const bool use_ref_pos_list) { const_cast<bool&>(_use_ref_pos_list) = use_ref_pos_list; };

 protected:
  void _consume(JitRuntimeContext& context) const final;

  const bool _use_ref_pos_list;
};

}  // namespace opossum
