// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/foreach.hpp>
#include <tr1/memory>

#include "tablet/tablet-util.h"

namespace kudu {
namespace tablet {
namespace tablet_util {

using std::tr1::shared_ptr;

Status CheckRowPresentInAnyLayer(const LayerVector &layers,
                                 const LayerKeyProbe &probe,
                                 bool *present) {
  *present = false;

  BOOST_FOREACH(const shared_ptr<LayerInterface> &layer, layers) {
    RETURN_NOT_OK(layer->CheckRowPresent(probe, present));
    if (*present) {
      break;
    }
  }

  return Status::OK();
}


} // namespace tablet_util
} // namespace tablet
} // namespace kudu
