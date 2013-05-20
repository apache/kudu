// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_TEST_UTIL_H
#define KUDU_TABLET_TABLET_TEST_UTIL_H

#include <string>
#include <vector>

#include "common/iterator.h"
#include "gutil/strings/join.h"

namespace kudu {
namespace tablet {

using std::string;
using std::vector;

static inline Status IterateToStringList(RowwiseIterator *iter,
                                         vector<string> *out,
                                         int limit = INT_MAX) {
  Schema schema = iter->schema();
  Arena arena(1024, 1024);
  RowBlock block(schema, 100, &arena);
  int fetched = 0;
  while (iter->HasNext() && fetched < limit) {
    RETURN_NOT_OK(RowwiseIterator::CopyBlock(iter, &block));
    for (size_t i = 0; i < block.nrows() && fetched < limit; i++) {
      if (block.selection_vector()->IsRowSelected(i)) {
        out->push_back( schema.DebugRow(block.row(i)) );
        fetched++;
      }
    }
  }
  std::sort(out->begin(), out->end());
  return Status::OK();
}

// Take an un-initialized iterator, Init() it, and iterate through all of its rows.
// The resulting string contains a line per entry.
static inline string InitAndDumpIterator(gscoped_ptr<RowwiseIterator> iter) {
  CHECK_OK(iter->Init(NULL));

  vector<string> out;
  CHECK_OK(IterateToStringList(iter.get(), &out));
  return JoinStrings(out, "\n");

}

} // namespace tablet
} // namespace kudu
#endif
