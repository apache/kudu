// Copyright (c) 2014, Cloudera, inc.

#include "consensus/opid_anchor_registry.h"

#include <boost/thread/locks.hpp>
#include <string>

#include "consensus/log_util.h"
#include "gutil/strings/substitute.h"

namespace kudu {
namespace log {

using consensus::OpId;
using std::pair;
using std::string;
using strings::Substitute;

OpIdAnchorRegistry::OpIdAnchorRegistry() {
}

void OpIdAnchorRegistry::Register(const OpId& op_id,
                                  const string& owner,
                                  OpIdAnchor* anchor) {
  anchor->op_id = op_id;
  anchor->owner = owner;
  anchor->is_registered = true;
  OpIdMultiMap::value_type value(op_id, anchor);

  boost::lock_guard<simple_spinlock> l(lock_);
  op_ids_.insert(value);
}

Status OpIdAnchorRegistry::Unregister(OpIdAnchor* anchor) {
  DCHECK(anchor != NULL);

  {
    boost::lock_guard<simple_spinlock> l(lock_);
    OpIdMultiMap::iterator iter = op_ids_.find(anchor->op_id);
    while (iter != op_ids_.end()) {
      if (iter->second == anchor) {
        OpIdMultiMap::iterator save = iter;
        ++save;
        op_ids_.erase(iter);
        iter = save;
        anchor->is_registered = false;
        return Status::OK();
      } else {
        ++iter;
      }
    }
  }

  return Status::NotFound(Substitute("OpId with key {$0} and owner $1 not found",
                                     anchor->op_id.ShortDebugString(), anchor->owner));
}

Status OpIdAnchorRegistry::GetEarliestRegisteredOpId(OpId* op_id) {
  boost::lock_guard<simple_spinlock> l(lock_);
  OpIdMultiMap::iterator iter = op_ids_.begin();
  if (iter == op_ids_.end()) {
    return Status::NotFound("No OpIds in registry");
  }

  // Since this is a sorted map, the first element is the one we want.
  *op_id = iter->first;
  return Status::OK();
}

size_t OpIdAnchorRegistry::GetAnchorCountForTests() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return op_ids_.size();
}

OpIdAnchor::OpIdAnchor()
  : is_registered(false) {
}

OpIdAnchor::~OpIdAnchor() {
  CHECK(!is_registered) << "Attempted to destruct a registered OpIdAnchor";
}

} // namespace log
} // namespace kudu
