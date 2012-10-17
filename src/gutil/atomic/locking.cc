// Copyright 2010 Google Inc. All Rights Reserved.

#include "gutil/macros.h"
#include "gutil/mutex.h"

namespace concurrent {
namespace detail {

Mutex seq_cst_mutex(base::LINKER_INITIALIZED);

}  // namespace detail
}  // namespace concurrent
