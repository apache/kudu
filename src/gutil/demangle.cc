// Copyright 2005 Google Inc. All Rights Reserved.

// We support certain compilers only.  See demangle.h for details.
#if (__GNUC__ >=4 || (__GNUC__ >= 3 && __GNUC_MINOR__ >= 4))
#  define HAS_CXA_DEMANGLE 1
#else
#  define HAS_CXA_DEMANGLE 0
#endif

#include <stdlib.h>
#if HAS_CXA_DEMANGLE
#include <cxxabi.h>
#endif
#include "gutil/demangle.h"
#include <glog/logging.h>
#include "gutil/logging-inl.h"

namespace util {
// The API reference of abi::__cxa_demangle() can be found in
// libstdc++'s manual.
// http://gcc.gnu.org/onlinedocs/libstdc++/latest-doxygen/namespaceabi.html
void DemangleToString(const char* mangled, string *out) {
  int status = 0;
  char *demangled = NULL;
#if HAS_CXA_DEMANGLE
  demangled = abi::__cxa_demangle(mangled, NULL, NULL, &status);
#endif
  if (status == 0 && demangled != NULL) {  // Demangling succeeeded.
    out->append(demangled);
    free(demangled);
  } else {
    out->append(mangled);
  }
}

string Demangle(const char *mangled) {
  string demangled;
  DemangleToString(mangled, &demangled);
  return demangled;
}

bool DemanglingIsSupported() {
  return HAS_CXA_DEMANGLE;
}
}
