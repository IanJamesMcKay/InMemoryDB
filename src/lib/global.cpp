#include "global.hpp"

namespace opossum {

// singleton
Global& Global::get() {
  static Global instance;
  return instance;
}

}  // namespace opossum
