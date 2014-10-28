// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Simple header which is inserted into all of our generated protobuf code.
// We use this to hook protobuf code up to TSAN annotations.
#ifndef KUDU_UTIL_PROTOBUF_ANNOTATIONS_H
#define KUDU_UTIL_PROTOBUF_ANNOTATIONS_H

#include "kudu/gutil/dynamic_annotations.h"

// The protobuf internal headers are included before this, so we have to undefine
// the empty definitions first.
#undef GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN
#undef GOOGLE_SAFE_CONCURRENT_WRITES_END

#define GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN ANNOTATE_IGNORE_WRITES_BEGIN
#define GOOGLE_SAFE_CONCURRENT_WRITES_END ANNOTATE_IGNORE_WRITES_END

#endif /* KUDU_UTIL_PROTOBUF_ANNOTATIONS_H */
