// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_SEMAPHORE_H
#define KUDU_UTIL_SEMAPHORE_H

#include <semaphore.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/monotime.h"

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

  // Get the current value of the semaphore.
  int GetValue();

  // Boost-compatible wrappers.
  void lock() { Acquire(); }
  void unlock() { Release(); }
  bool try_lock() { return TryAcquire(); }

 private:
  // Log a fatal error message. Separated out to keep the main functions
  // as small as possible in terms of code size.
  void Fatal(const char* action) ATTRIBUTE_NORETURN;

  sem_t sem_;
  DISALLOW_COPY_AND_ASSIGN(Semaphore);
};

} // namespace kudu
#endif /* KUDU_UTIL_SEMAPHORE_H */
