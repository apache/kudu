// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.
//
// Utility methods for dealing with string case.
#ifndef KUDU_UTIL_STRING_CASE_H
#define KUDU_UTIL_STRING_CASE_H

#include <string>

namespace kudu {

// Convert the given snake_case string to camel case.
// Also treats '-' in a string like a '_'
// For example:
// - 'foo_bar' -> FooBar
// - 'foo-bar' -> FooBar
//
// This function cannot operate in-place -- i.e. 'camel_case' must not
// point to 'snake_case'.
void SnakeToCamelCase(const std::string &snake_case,
                      std::string *camel_case);

// Upper-case all of the characters in the given string.
// 'string' and 'out' may refer to the same string to replace in-place.
void ToUpperCase(const std::string &string,
                 std::string *out);

// Capitalizes a string containing a word in place.
// For example:
// - 'hiBerNATe' -> 'Hibernate'
void Capitalize(std::string *word);

} // namespace kudu
#endif
