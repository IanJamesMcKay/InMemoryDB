#pragma once

#include <iostream>

namespace opossum {

// Used to config out()
extern bool verbose;

/**
 * @return std::cout if `verbose` is true, otherwise returns a discarding stream
 */
inline std::ostream &out() {
  if (verbose) {
    return std::cout;
  }

  // Create no-op stream that just swallows everything streamed into it
  // See https://stackoverflow.com/a/11826666
  class NullBuffer : public std::streambuf {
   public:
    int overflow(int c) { return c; }
  };

  static NullBuffer null_buffer;
  static std::ostream null_stream(&null_buffer);

  return null_stream;
}

}  // namespace opossum
