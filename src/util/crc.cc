// Copyright (c) 2014 Cloudera Inc.
#include "util/crc.h"

#include <crcutil/interface.h>
#include <gperftools/heap-checker.h>

#include "gutil/once.h"

namespace kudu {
namespace crc {

static GoogleOnceType crc32c_once = GOOGLE_ONCE_INIT;
static Crc* crc32c_instance = NULL;

static void InitCrc32cInstance() {
#ifdef TCMALLOC_ENABLED
  HeapLeakChecker::Disabler disabler; // CRC instance is never freed.
#endif // TCMALLOC_ENABLED
  // TODO: Is initial = 0 and roll window = 4 appropriate for all cases?
  crc32c_instance = crcutil_interface::CRC::CreateCrc32c(true, 0, 4, NULL);
}

Crc* GetCrc32cInstance() {
  GoogleOnceInit(&crc32c_once, &InitCrc32cInstance);
  return crc32c_instance;
}

} // namespace crc
} // namespace kudu
