// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_CONSENSUS_REF_COUNTED_REPLICATE_H_
#define KUDU_CONSENSUS_REF_COUNTED_REPLICATE_H_

#include "kudu/consensus/consensus.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/gscoped_ptr.h"

namespace kudu {
namespace consensus {

// A simple ref-counted wrapper around ReplicateMsg.
class RefCountedReplicate : public RefCountedThreadSafe<RefCountedReplicate> {
 public:
  explicit RefCountedReplicate(ReplicateMsg* msg) : msg_(msg) {}

  ReplicateMsg* get() {
    return msg_.get();
  }

 private:
  gscoped_ptr<ReplicateMsg> msg_;
};

typedef scoped_refptr<RefCountedReplicate> ReplicateRefPtr;

inline ReplicateRefPtr make_scoped_refptr_replicate(ReplicateMsg* replicate) {
  return ReplicateRefPtr(new RefCountedReplicate(replicate));
}

} // namespace consensus
} // namespace kudu

#endif /* KUDU_CONSENSUS_REF_COUNTED_REPLICATE_H_ */
