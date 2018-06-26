#include "timer.hpp"

namespace opossum {

Timer::Timer() { _begin = std::chrono::high_resolution_clock::now(); }

Timer::Timer(const Callback& on_destruct): _on_destruct(on_destruct) {
  _begin = std::chrono::high_resolution_clock::now();
}

Timer::~Timer() {
  if (_on_destruct) {
    const auto duration = lap();
    (*_on_destruct)(duration);
  }
}

std::chrono::microseconds Timer::lap() {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto lap_ns = std::chrono::duration_cast<std::chrono::microseconds>(now - _begin);
  _begin = now;
  return lap_ns;
}

}  // namespace opossum
