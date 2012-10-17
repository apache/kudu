// Copyright 2005 Google Inc. All Rights Reserved.
//
// Functions for demangling symbol names.  This works by calling a
// demangler API defined by C++ ABI if available.
//
// Reference:
//
//  http://www.codesourcery.com/cxx-abi/
//  http://www.codesourcery.com/cxx-abi/abi.html#demangler

#ifndef UTIL_SYMBOLIZE_DEMANGLE_H__
#define UTIL_SYMBOLIZE_DEMANGLE_H__

#include <string>
using std::string;
#include "gutil/basictypes.h"

namespace util {

// Demangle a mangled symbol name and return the demangled name.
// If 'mangled' isn't mangled in the first place, this function
// simply returns 'mangled' as is.
//
// This function is used for demangling mangled symbol names such as
// '_Z3bazifdPv'.  It uses abi::__cxa_demangle() if your compiler has
// the API.  Otherwise, this function simply returns 'mangled' as is.
//
// Currently, we support only GCC 3.4.x or later for the following
// reasons.
//
// - GCC 2.95.3 doesn't have cxxabi.h
// - GCC 3.3.5 and ICC 9.0 have a bug.  Their abi::__cxa_demangle()
//   returns junk values for non-mangled symbol names (ex. function
//   names in C linkage).  For example,
//
//     abi::__cxa_demangle("main", 0,  0, &status)
//
//   returns "unsigned long" and the status code is 0 (successful).
//   Ugh...  Maybe they share the same broken code.
string Demangle(const char* mangled);

// Same function as above but appends result to 'out'.
void DemangleToString(const char* mangled, string *out);

// Return true if demangling is supported.
// Return false otherwise.  Mainly used for testing.
bool DemanglingIsSupported();
}

#endif  // UTIL_SYMBOLIZE_DEMANGLE_H__
