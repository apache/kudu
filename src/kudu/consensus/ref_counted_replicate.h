// Copyright 2013 Cloudera, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
