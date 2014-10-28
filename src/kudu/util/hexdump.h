// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_HEXDUMP_H
#define KUDU_UTIL_HEXDUMP_H

#include <string>

namespace kudu {

class Slice;

// Generate an 'xxd'-style hexdump of the given slice.
// This should only be used for debugging, as the format is
// subject to change and it has not been implemented for
// speed.
std::string HexDump(const Slice &slice);

} // namespace kudu
#endif
