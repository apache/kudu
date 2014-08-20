// Copyright (c) 2014, Cloudera, inc.

#include "kudu/consensus/opid_anchor_registry.h"

#include <boost/thread/locks.hpp>
#include <string>

#include "kudu/gutil/strings/substitute.h"

namespace kudu {
namespace log {

using consensus::OpId;
using std::pair;
using std::string;
using strings::Substitute;

OpIdAnchorRegistry::OpIdAnchorRegistry() {
}
OpIdAnchorRegistry::~OpIdAnchorRegistry() {
  CHECK(op_ids_.empty());
}

void OpIdAnchorRegistry::Register(const OpId& op_id,
                                  const string& owner,
                                  OpIdAnchor* anchor) {
  boost::lock_guard<simple_spinlock> l(lock_);
  RegisterUnlocked(op_id, owner, anchor);
}

Status OpIdAnchorRegistry::UpdateRegistration(const consensus::OpId& op_id,
                                              const std::string& owner,
                                              OpIdAnchor* anchor) {
  boost::lock_guard<simple_spinlock> l(lock_);
  RETURN_NOT_OK_PREPEND(UnregisterUnlocked(anchor),
                        "Unable to swap registration, anchor not registered")
  RegisterUnlocked(op_id, owner, anchor);
  return Status::OK();
}

Status OpIdAnchorRegistry::Unregister(OpIdAnchor* anchor) {
  boost::lock_guard<simple_spinlock> l(lock_);
  return UnregisterUnlocked(anchor);
}

bool OpIdAnchorRegistry::IsRegistered(OpIdAnchor* anchor) const {
  return anchor->is_registered;
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

void OpIdAnchorRegistry::RegisterUnlocked(const consensus::OpId& op_id,
                                          const std::string& owner,
                                          OpIdAnchor* anchor) {
  DCHECK(anchor != NULL);
  DCHECK(!anchor->is_registered);

  anchor->op_id.CopyFrom(op_id);
  anchor->owner.assign(owner);
  anchor->is_registered = true;
  OpIdMultiMap::value_type value(op_id, anchor);
  op_ids_.insert(value);
}

Status OpIdAnchorRegistry::UnregisterUnlocked(OpIdAnchor* anchor) {
  DCHECK(anchor != NULL);
  DCHECK(anchor->is_registered);

  OpIdMultiMap::iterator iter = op_ids_.find(anchor->op_id);
  while (iter != op_ids_.end()) {
    if (iter->second == anchor) {
      anchor->is_registered = false;
      op_ids_.erase(iter);
      // No need for the iterator to remain valid since we return here.
      return Status::OK();
    } else {
      ++iter;
    }
  }
  return Status::NotFound(Substitute("OpId with key {$0} and owner $1 not found",
                                     anchor->op_id.ShortDebugString(), anchor->owner));
}

OpIdAnchor::OpIdAnchor()
  : is_registered(false) {
}

OpIdAnchor::~OpIdAnchor() {
  CHECK(!is_registered) << "Attempted to destruct a registered OpIdAnchor";
}

OpIdMinAnchorer::OpIdMinAnchorer(OpIdAnchorRegistry* registry, const string& owner)
  : registry_(DCHECK_NOTNULL(registry)),
    owner_(owner) {
}

OpIdMinAnchorer::~OpIdMinAnchorer() {
  CHECK_OK(ReleaseAnchor());
}

void OpIdMinAnchorer::AnchorIfMinimum(const consensus::OpId& op_id) {
  boost::lock_guard<simple_spinlock> l(lock_);
  if (PREDICT_FALSE(!minimum_op_id_.IsInitialized())) {
    minimum_op_id_.CopyFrom(op_id);
    registry_->Register(minimum_op_id_, owner_, &anchor_);
  } else if (OpIdLessThan(op_id, minimum_op_id_)) {
    minimum_op_id_.CopyFrom(op_id);
    CHECK_OK(registry_->UpdateRegistration(minimum_op_id_, owner_, &anchor_));
  }
}

Status OpIdMinAnchorer::ReleaseAnchor() {
  boost::lock_guard<simple_spinlock> l(lock_);
  if (PREDICT_TRUE(minimum_op_id_.IsInitialized())) {
    return registry_->Unregister(&anchor_);
  }
  return Status::OK(); // If there were no inserts, return OK.
}

} // namespace log
} // namespace kudu
