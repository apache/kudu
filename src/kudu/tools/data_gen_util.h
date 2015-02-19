// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Utility functions for generating data for use by tools and tests.

#ifndef KUDU_TOOLS_DATA_GEN_UTIL_H_
#define KUDU_TOOLS_DATA_GEN_UTIL_H_

#include <stdint.h>

namespace kudu {
class KuduPartialRow;
class Random;

namespace client {
class KuduSchema;
} // namespace client

namespace tools {

// Detect the type of the given column and coerce the given number value in
// 'value' to the data type of that column.
// At the time of this writing, we only support ints, bools, and strings.
// For the numbers / bool, the value is truncated to fit the data type.
// For the string, we encode the number as hex.
void WriteValueToColumn(const client::KuduSchema& schema,
                        int col_idx,
                        uint64_t value,
                        KuduPartialRow* row);

// Generate row data for an arbitrary schema. Initial column value determined
// by the value of 'record_id'.
void GenerateDataForRow(const client::KuduSchema& schema, uint64_t record_id,
                        Random* random, KuduPartialRow* row);

} // namespace tools
} // namespace kudu

#endif // KUDU_TOOLS_DATA_GEN_UTIL_H_
