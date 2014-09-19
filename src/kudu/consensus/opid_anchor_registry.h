// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CONSENSUS_OPID_ANCHOR_REGISTRY_
#define KUDU_CONSENSUS_OPID_ANCHOR_REGISTRY_

#include <map>
#include <string>
#include <gtest/gtest_prod.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {
namespace log {

class OpIdAnchor;

// This class allows callers to register their interest in (anchor) a particular
// OpId. The primary use case for this is to prevent the deletion of segments of
// the WAL that reference as-yet unflushed in-memory operations.
//
// This class is thread-safe.
class OpIdAnchorRegistry : public RefCountedThreadSafe<OpIdAnchorRegistry> {
 public:
  OpIdAnchorRegistry();

  // Register interest for a particular OpId.
  // op_id: The OpId the caller wishes to anchor.
  // owner: String to describe who is registering the anchor. Used in assert
  //        messages for debugging purposes.
  // anchor: Pointer to OpIdAnchor structure that will be populated on registration.
  void Register(const consensus::OpId& op_id, const std::string& owner, OpIdAnchor* anchor);

  // Atomically update the registration of an anchor to a new OpId.
  // Before: anchor must be registered with some OpId.
  // After: anchor is now registered using OpId op_id.
  // See Register().
  Status UpdateRegistration(const consensus::OpId& op_id,
                            const std::string& owner,
                            OpIdAnchor* anchor);

  // Release the anchor on an OpId.
  // Note: anchor must be the original pointer passed to Register().
  Status Unregister(OpIdAnchor* anchor);

  // Returns true if passed anchor is currently registered.
  bool IsRegistered(OpIdAnchor* anchor) const;

  // Query the registry to find the earliest anchored OpId in the registry.
  // Returns Status::NotFound if no anchors are currently active.
  Status GetEarliestRegisteredOpId(consensus::OpId* op_id);

  // Simply returns the number of active anchors for use in debugging / tests.
  // This is _not_ a constant-time operation.
  size_t GetAnchorCountForTests() const;

 private:
  friend class RefCountedThreadSafe<OpIdAnchorRegistry>;
  ~OpIdAnchorRegistry();

  typedef std::multimap<consensus::OpId,
                        OpIdAnchor*,
                        consensus::OpIdCompareFunctor> OpIdMultiMap;

  // Register a new anchor after taking the lock. See Register().
  void RegisterUnlocked(const consensus::OpId& op_id, const std::string& owner, OpIdAnchor* anchor);

  // Unregister an anchor after taking the lock. See Unregister().
  Status UnregisterUnlocked(OpIdAnchor* anchor);

  OpIdMultiMap op_ids_;
  mutable simple_spinlock lock_;

  DISALLOW_COPY_AND_ASSIGN(OpIdAnchorRegistry);
};

// An opaque class that helps us keep track of anchors.
class OpIdAnchor {
 public:
  OpIdAnchor();
  ~OpIdAnchor();

 private:
  FRIEND_TEST(LogTest, TestGCWithLogRunning);
  friend class OpIdAnchorRegistry;

  consensus::OpId op_id;
  std::string owner;
  bool is_registered;

  DISALLOW_COPY_AND_ASSIGN(OpIdAnchor);
};

// Helper class that will anchor the minimum OpId recorded.
class OpIdMinAnchorer {
 public:
  // Construct anchorer for specified registry that will register anchors with
  // the specified owner name.
  OpIdMinAnchorer(OpIdAnchorRegistry* registry, const std::string& owner);

  // The destructor will unregister the anchor if it is registered.
  ~OpIdMinAnchorer();

  // If op_id is less than the minimum OpId registered so far, or if no OpIds
  // are currently registered, anchor on op_id.
  void AnchorIfMinimum(const consensus::OpId& op_id);

  // Un-anchors the earliest OpId (which is the only one tracked).
  // If no minimum is known (no OpId registered), returns OK.
  Status ReleaseAnchor();

 private:
  scoped_refptr<OpIdAnchorRegistry> const registry_;
  const std::string owner_;
  OpIdAnchor anchor_;
  consensus::OpId minimum_op_id_;
  mutable simple_spinlock lock_;

  DISALLOW_COPY_AND_ASSIGN(OpIdMinAnchorer);
};

} // namespace log
} // namespace kudu

#endif // KUDU_CONSENSUS_OPID_ANCHOR_REGISTRY_
