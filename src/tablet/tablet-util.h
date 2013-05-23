// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_TABLET_UTIL_H
#define KUDU_TABLET_TABLET_UTIL_H

#include "tablet/diskrowset.h"
#include "tablet/rowset.h"

namespace kudu {
namespace tablet {
namespace tablet_util {

// Set *present to indicate whether the given key is present in any of the
// rowsets in 'rowsets'.
Status CheckRowPresentInAnyRowSet(const RowSetVector &rowsets,
                                 const RowSetKeyProbe &probe,
                                 bool *present);



} // namespace tablet_util
} // namespace tablet
} // namespace kudu

#endif
