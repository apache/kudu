// Copyright 2001 - 2003 Google, Inc.
//
// Google-specific types

#ifndef BASE_BASICTYPES_H_
#define BASE_BASICTYPES_H_

#include "gutil/integral_types.h"
#include "gutil/macros.h"

// Argument type used in interfaces that can optionally take ownership
// of a passed in argument.  If TAKE_OWNERSHIP is passed, the called
// object takes ownership of the argument.  Otherwise it does not.
enum Ownership {
  DO_NOT_TAKE_OWNERSHIP,
  TAKE_OWNERSHIP
};

#endif  // BASE_BASICTYPES_H_
