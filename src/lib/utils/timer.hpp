#pragma once

#include <chrono>
#include <functional>

namespace opossum {

/**
 * Starts a std::chrono::high_resolution_clock base timer on construction and returns/reset measurement on calling lap()
 */
class Timer final {
 public:
  using Callback = std::function<void(const std::chrono::microseconds&)>;

  Timer();
  Timer(const Callback& on_destruct);
  ~Timer();

  std::chrono::microseconds lap();

 private:
  std::chrono::high_resolution_clock::time_point _begin;
  std::optional<Callback> _on_destruct;
};

}  // namespace opossum