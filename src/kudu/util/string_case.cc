// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.

#include "kudu/util/string_case.h"

#include <glog/logging.h>
#include <ctype.h>

namespace kudu {

using std::string;

void SnakeToCamelCase(const std::string &snake_case,
                      std::string *camel_case) {
  DCHECK_NE(camel_case, &snake_case) << "Does not support in-place operation";
  camel_case->clear();
  camel_case->reserve(snake_case.size());

  bool uppercase_next = true;
  for (int i = 0; i < snake_case.size(); i++) {
    char c = snake_case[i];
    if ((c == '_') ||
        (c == '-')) {
      uppercase_next = true;
      continue;
    }
    if (uppercase_next) {
      camel_case->push_back(toupper(c));
    } else {
      camel_case->push_back(c);
    }
    uppercase_next = false;
  }
}

void ToUpperCase(const std::string &string,
                 std::string *out) {
  if (out != &string) {
    *out = string;
  }

  for (int i = 0; i < out->size(); i++) {
    (*out)[i] = toupper((*out)[i]);
  }
}

void Capitalize(string *word) {
  uint32_t size = word->size();
  if (size == 0) {
    return;
  }

  (*word)[0] = toupper((*word)[0]);

  for (int i = 1; i < size; i++) {
    (*word)[i] = tolower((*word)[i]);
  }
}

} // namespace kudu
