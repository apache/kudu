// Copyright (c) 2013, Cloudera, inc.

#include "tablet/mutation.h"
#include <string>

namespace kudu {
namespace tablet {

string Mutation::StringifyMutationList(const Schema &schema, const Mutation *head) {
  string ret;

  ret.append("[");

  bool first = true;
  while (head != NULL) {
    if (!first) {
      ret.append(", ");
    }
    first = false;

    RowChangeListDecoder decoder(schema, head->changelist_slice());
    StringAppendF(&ret, "@%"TXID_PRINT_FORMAT"(", head->txid().v);
    ret.append("@(");
    ret.append(decoder.ToString());
    ret.append(")");

    head = head->next();
  }

  ret.append("]");
  return ret;
}

} // namespace tablet
} // namespace kudu
