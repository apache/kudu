// Copyright (c) 2012, Cloudera,inc.

#include <algorithm>
#include <string>

#include "hexdump.h"
#include "slice.h"
#include "gutil/stringprintf.h"

namespace kudu {

std::string HexDump(const Slice &slice) {

  std::string output;
  output.reserve(slice.size() * 5);

  const char *p = slice.data();

  int rem = slice.size();
  while (rem > 0) {
    const char *line_p = p;
    int line_len = std::min(rem, 16);
    int line_rem = line_len;
    StringAppendF(&output, "%06lx: ", line_p - slice.data());

    while (line_rem >= 2) {
      StringAppendF(&output, "%02x%02x ",
                    p[0] & 0xff, p[1] & 0xff);
      p += 2;
      line_rem -= 2;
    }

    if (line_rem == 1) {
      StringAppendF(&output, "%02x   ",
                    p[0] & 0xff);
      p += 1;
      line_rem -= 1;
    }

    int padding = (16 - line_len) / 2;

    for (int i = 0; i < padding; i++) {
      output.append("     ");
    }

    for (int i = 0; i < line_len; i++) {
      char c = line_p[i];
      if (isprint(c)) {
        output.push_back(c);
      } else {
        output.push_back('.');
      }
    }

    output.push_back('\n');
    rem -= line_len;
  }
  return output;
}

}
