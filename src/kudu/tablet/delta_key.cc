// Copyright (c) 2014, Cloudera, inc.

#include "kudu/tablet/delta_key.h"

#include <glog/logging.h>

namespace kudu {
namespace tablet {

const char* DeltaType_Name(DeltaType t) {
  switch (t) {
    case UNDO:
      return "UNDO";
    case REDO:
      return "REDO";
    default:
      LOG(DFATAL) << "Unknown delta type: " << t;
  }
  return "UNKNOWN";
}

} // namespace tablet
} // namespace kudu
