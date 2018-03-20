#pragma once

#include <cmath>
#include <string>

namespace opossum {

template<typename Integer>
std::string format_integer(Integer integer) {
  static_assert(std::is_integral_v<Integer>, "Expected integral parameter");

  auto i = 0; // haha, I named a variable "i"

  if constexpr(!std::is_unsigned_v<Integer>) {
    while (std::abs(integer) > 10'000 && i < 3) {
      integer /= 1000;
      ++i;
    }
  } else {
    while (integer > 10'000 && i < 3) {
      integer /= 1000;
      ++i;
    }
  }

  auto str = std::to_string(integer);
  switch (i) {
    case 1: str += "K"; break;
    case 2: str += "M"; break;
    case 3: str += "B"; break;
  }

  return str;
}

}