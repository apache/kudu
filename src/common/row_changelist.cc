// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <string>
#include "common/row_changelist.h"
#include "common/schema.h"

namespace kudu {

string RowChangeList::ToString(const Schema &schema) const {
  RowChangeListDecoder decoder(schema, *this);

  Status s = decoder.Init();
  if (!s.ok()) {
    return "[invalid: " + s.ToString() + "]";
  }

  if (decoder.is_delete()) {
    return string("DELETE");
  } else if (decoder.is_reinsert()) {
    ConstContiguousRow row(schema, decoder.remaining_.data());
    return string("REINSERT ") + schema.DebugRow(row);
  } else {
    CHECK(decoder.is_update()) << "Unknown changelist type!";
  }

  string ret = "SET ";

  bool first = true;
  while (decoder.HasNext()) {
    if (!first) {
      ret.append(", ");
    }
    first = false;

    size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
    const void *new_val = NULL;
    s = decoder.DecodeNext(&updated_col, &new_val);
    if (!s.ok()) {
      return "[invalid: " + s.ToString() + ", before corruption: " + ret + "]";
    }

    ret.append(schema.column(updated_col).name());
    ret.append("=");
    ret.append(schema.column(updated_col).Stringify(new_val));
  }

  return ret;
}

} // namespace kudu
