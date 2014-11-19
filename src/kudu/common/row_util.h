// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_COMMON_ROW_UTIL_H
#define KUDU_COMMON_ROW_UTIL_H

#include <string>

namespace kudu {

class Schema;
class KuduPartialRow;

// Convert a KuduPartialRow to a stringified form for debugging.
std::string DebugPartialRowToString(const KuduPartialRow& row);

// Convert a particular column value in a row to a string for debugging.
std::string DebugColumnValueToString(const KuduPartialRow& row,
                                     int col_idx);

} // namespace kudu
#endif /* KUDU_COMMON_ROW_UTIL_H */
