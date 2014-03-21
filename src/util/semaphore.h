// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_SEMAPHORE_H
#define KUDU_UTIL_SEMAPHORE_H

#include <semaphore.h>

#include "gutil/macros.h"
#include "gutil/port.h"
#include "util/monotime.h"

namespace kudu {

// Wrapper for POSIX semaphores.
class Semaphore {
 public:
  // Initialize the semaphore with the specified capacity.
  explicit Semaphore(int capacity);
  ~Semaphore();

  // Acquire the semaphore.
  void Acquire();

  // Acquire the semaphore within the given timeout. Returns true if successful.
  bool TimedAcquire(const MonoDelta& timeout);

  // Try to acquire the semaphore immediately. Returns false if unsuccessful.
  bool TryAcquire();

  // Release the semaphore.
  void Release();

 private:
  // Log a fatal error message. Separated out to keep the main functions
  // as small as possible in terms of code size.
  void Fatal(const char* action) ATTRIBUTE_NORETURN;

  sem_t sem_;
  DISALLOW_COPY_AND_ASSIGN(Semaphore);
};

} // namespace kudu
#endif /* KUDU_UTIL_SEMAPHORE_H */
