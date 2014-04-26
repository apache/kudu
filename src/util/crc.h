// Copyright (c) 2014 Cloudera Inc.
#ifndef KUDU_UTIL_CRC_H_
#define KUDU_UTIL_CRC_H_

#include <stdint.h>
#include <stdlib.h>

#include <crcutil/interface.h>

namespace kudu {
namespace crc {

typedef crcutil_interface::CRC Crc;

// Returns pointer to singleton instance of CRC32C implementation.
Crc* GetCrc32cInstance();

} // namespace crc
} // namespace kudu

#endif // KUDU_UTIL_CRC_H_
