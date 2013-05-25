// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <string>
#include "common/row_changelist.h"

namespace kudu {

string RowChangeList::ToString(const Schema &schema) const {
  RowChangeListDecoder decoder(schema, *this);
  string ret = "SET ";

  bool first = true;
  while (decoder.HasNext()) {
    if (!first) {
      ret.append(", ");
    }
    first = false;

    size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
    const void *new_val = NULL;
    CHECK_OK(decoder.DecodeNext(&updated_col, &new_val));

    const ColumnSchema& col_schema = schema.column(updated_col);
    ret.append(col_schema.name());
    ret.append("=");
    if (col_schema.is_nullable() && new_val == NULL) {
      ret.append("NULL");
    } else {
      ret.append(col_schema.Stringify(new_val));
    }
  }

  return ret;
}

} // namespace kudu
