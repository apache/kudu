// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/foreach.hpp>
#include <tr1/memory>

#include "tablet/tablet-util.h"

namespace kudu {
namespace tablet {
namespace tablet_util {

using std::tr1::shared_ptr;

Status CheckRowPresentInAnyRowSet(const RowSetVector &rowsets,
                                 const RowSetKeyProbe &probe,
                                 bool *present) {
  *present = false;

  BOOST_FOREACH(const shared_ptr<RowSetInterface> &rowset, rowsets) {
    RETURN_NOT_OK(rowset->CheckRowPresent(probe, present));
    if (*present) {
      break;
    }
  }

  return Status::OK();
}


} // namespace tablet_util
} // namespace tablet
} // namespace kudu
