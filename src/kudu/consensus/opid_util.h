// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_OPID_UTIL_H_
#define KUDU_CONSENSUS_OPID_UTIL_H_

#include <stdint.h>

#include <iosfwd>
#include <utility>

namespace kudu {
namespace consensus {

class OpId;

// Minimum possible term.
extern const uint64_t kMinimumTerm;

// Returns true iff left == right.
bool OpIdEquals(const OpId& left, const OpId& right);

// Returns true iff left < right.
bool OpIdLessThan(const OpId& left, const OpId& right);

// Returns true iff left > right.
bool OpIdBiggerThan(const OpId& left, const OpId& right);

// Copies to_compare into target under the following conditions:
// - If to_compare is initialized and target is not.
// - If they are both initialized and to_compare is less than target.
// Otherwise, does nothing.
// If to_compare is copied into target, returns true, else false.
bool CopyIfOpIdLessThan(const OpId& to_compare, OpId* target);

// Return -1, 0, or 1.
int OpIdCompare(const OpId& left, const OpId& right);

// OpId hash functor. Suitable for use with std::unordered_map.
struct OpIdHashFunctor {
  size_t operator() (const OpId& id) const;
};

// OpId equals functor. Suitable for use with std::unordered_map.
struct OpIdEqualsFunctor {
  bool operator() (const OpId& left, const OpId& right) const;
};

// OpId compare() functor. Suitable for use with std::sort and std::map.
struct OpIdCompareFunctor {
  // Returns true iff left < right.
  bool operator() (const OpId& left, const OpId& right) const;
};

// OpId comparison functor that returns true iff left > right. Suitable for use
// td::sort and std::map to sort keys in increasing order.]
struct OpIdBiggerThanFunctor {
  bool operator() (const OpId& left, const OpId& right) const;
};

std::ostream& operator<<(std::ostream& os, const consensus::OpId& op_id);

// Return the minimum possible OpId.
OpId MinimumOpId();

// Return the maximum possible OpId.
OpId MaximumOpId();

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_OPID_UTIL_H_ */
