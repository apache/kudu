// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/tablet/mutation.h"
#include <string>

namespace kudu {
namespace tablet {

string Mutation::StringifyMutationList(const Schema &schema, const Mutation *head) {
  string ret;

  ret.append("[");

  bool first = true;
  while (head != NULL) {
    if (!first) {
      ret.append(", ");
    }
    first = false;

    StrAppend(&ret, "@", head->timestamp().ToString(), "(");
    ret.append(head->changelist().ToString(schema));
    ret.append(")");

    head = head->next();
  }

  ret.append("]");
  return ret;
}


void Mutation::AppendToListAtomic(Mutation **list) {
  DoAppendToList<true>(list);
}

void Mutation::AppendToList(Mutation **list) {
  DoAppendToList<false>(list);
}

namespace {
template<bool ATOMIC>
inline void Store(Mutation** pointer, Mutation* val);

template<>
inline void Store<true>(Mutation** pointer, Mutation* val) {
  Release_Store(reinterpret_cast<AtomicWord*>(pointer),
                reinterpret_cast<AtomicWord>(val));
}

template<>
inline void Store<false>(Mutation** pointer, Mutation* val) {
  *pointer = val;
}
} // anonymous namespace

template<bool ATOMIC>
inline void Mutation::DoAppendToList(Mutation **list) {
  next_ = NULL;
  if (*list == NULL) {
    Store<ATOMIC>(list, this);
  } else {
    // Find tail and append.
    Mutation *tail = *list;
    while (tail->next_ != NULL) {
      tail = tail->next_;
    }
    Store<ATOMIC>(&tail->next_, this);
  }
}

} // namespace tablet
} // namespace kudu
