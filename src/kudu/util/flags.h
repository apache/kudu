// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_FLAGS_H
#define KUDU_UTIL_FLAGS_H

#include "kudu/gutil/macros.h"

namespace kudu {

// Looks for flags in argv and parses them.  Rearranges argv to put
// flags first, or removes them entirely if remove_flags is true.
// If a flag is defined more than once in the command line or flag
// file, the last definition is used.  Returns the index (into argv)
// of the first non-flag argument.
//
// This is a wrapper around google::ParseCommandLineFlags, but integrates
// with Kudu flag tags. For example, --helpxml will include the list of
// tags for each flag. This should be be used instead of
// google::ParseCommandLineFlags in any user-facing binary.
//
// See gflags.h for more information.
int ParseCommandLineFlags(int* argc, char*** argv, bool remove_flags);

} // namespace kudu
#endif /* KUDU_UTIL_FLAGS_H */
