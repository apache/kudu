// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CONSENSUS_OPID_ANCHOR_REGISTRY_
#define KUDU_CONSENSUS_OPID_ANCHOR_REGISTRY_

#include <map>
#include <string>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {
namespace log {

class OpIdAnchor;

// This class allows callers to register their interest in (anchor) a particular
// log index. The primary use case for this is to prevent the deletion of segments of
// the WAL that reference as-yet unflushed in-memory operations.
//
// TODO: rename this to LogAnchorRegistry, since it is based on log indexes rather than
// OpIds.
//
// This class is thread-safe.
class OpIdAnchorRegistry : public RefCountedThreadSafe<OpIdAnchorRegistry> {
 public:
  OpIdAnchorRegistry();

  // Register interest for a particular log index.
  // log_index: The log index the caller wishes to anchor.
  // owner: String to describe who is registering the anchor. Used in assert
  //        messages for debugging purposes.
  // anchor: Pointer to OpIdAnchor structure that will be populated on registration.
  void Register(int64_t log_index, const std::string& owner, OpIdAnchor* anchor);

  // Atomically update the registration of an anchor to a new log index.
  // Before: anchor must be registered with some log index.
  // After: anchor is now registered using index 'log_index'.
  // See Register().
  Status UpdateRegistration(int64_t log_index,
                            const std::string& owner,
                            OpIdAnchor* anchor);

  // Release the anchor on a log index.
  // Note: anchor must be the original pointer passed to Register().
  Status Unregister(OpIdAnchor* anchor);

  // Returns true if passed anchor is currently registered.
  bool IsRegistered(OpIdAnchor* anchor) const;

  // Query the registry to find the earliest anchored log index in the registry.
  // Returns Status::NotFound if no anchors are currently active.
  Status GetEarliestRegisteredLogIndex(int64_t* op_id);

  // Simply returns the number of active anchors for use in debugging / tests.
  // This is _not_ a constant-time operation.
  size_t GetAnchorCountForTests() const;

 private:
  friend class RefCountedThreadSafe<OpIdAnchorRegistry>;
  ~OpIdAnchorRegistry();

  typedef std::multimap<int64_t, OpIdAnchor*> AnchorMultiMap;

  // Register a new anchor after taking the lock. See Register().
  void RegisterUnlocked(int64_t log_index, const std::string& owner, OpIdAnchor* anchor);

  // Unregister an anchor after taking the lock. See Unregister().
  Status UnregisterUnlocked(OpIdAnchor* anchor);

  AnchorMultiMap anchors_;
  mutable simple_spinlock lock_;

  DISALLOW_COPY_AND_ASSIGN(OpIdAnchorRegistry);
};

// An opaque class that helps us keep track of anchors.
// TODO: rename to LogAnchor
class OpIdAnchor {
 public:
  OpIdAnchor();
  ~OpIdAnchor();

 private:
  FRIEND_TEST(LogTest, TestGCWithLogRunning);
  friend class OpIdAnchorRegistry;

  int64_t log_index;
  std::string owner;
  bool is_registered;

  DISALLOW_COPY_AND_ASSIGN(OpIdAnchor);
};

// Helper class that will anchor the minimum log index recorded.
class OpIdMinAnchorer {
 public:
  // Construct anchorer for specified registry that will register anchors with
  // the specified owner name.
  OpIdMinAnchorer(OpIdAnchorRegistry* registry, const std::string& owner);

  // The destructor will unregister the anchor if it is registered.
  ~OpIdMinAnchorer();

  // If op_id is less than the minimum index registered so far, or if no indexes
  // are currently registered, anchor on 'log_index'.
  void AnchorIfMinimum(int64_t log_index);

  // Un-anchors the earliest index (which is the only one tracked).
  // If no minimum is known (no anchor registered), returns OK.
  Status ReleaseAnchor();

 private:
  scoped_refptr<OpIdAnchorRegistry> const registry_;
  const std::string owner_;
  OpIdAnchor anchor_;

  // The index currently anchored, or -1 if no anchor has yet been registered.
  int64_t minimum_log_index_;
  mutable simple_spinlock lock_;

  DISALLOW_COPY_AND_ASSIGN(OpIdMinAnchorer);
};

} // namespace log
} // namespace kudu

#endif // KUDU_CONSENSUS_OPID_ANCHOR_REGISTRY_
