// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Simple pool of objects that will be deallocated when the pool is
// destroyed

#ifndef KUDU_UTIL_AUTO_RELEASE_POOL_H
#define KUDU_UTIL_AUTO_RELEASE_POOL_H

#include <vector>

#include "kudu/gutil/spinlock.h"

namespace kudu {

// Thread-safe.
class AutoReleasePool {
 public:
  AutoReleasePool(): objects_() { }

  ~AutoReleasePool() {
    for (ElementVector::iterator i = objects_.begin();
         i != objects_.end(); ++i) {
      delete *i;
    }
  }

  template <class T>
  T *Add(T *t) {
    base::SpinLockHolder l(&lock_);
    objects_.push_back(new SpecificElement<T>(t));
    return t;
  }

  // Add an array-allocated object to the pool. This is identical to
  // Add() except that it will be freed with 'delete[]' instead of 'delete'.
  template<class T>
  T* AddArray(T *t) {
    base::SpinLockHolder l(&lock_);
    objects_.push_back(new SpecificArrayElement<T>(t));
    return t;
  }

  // Donate all objects in this pool to another pool.
  void DonateAllTo(AutoReleasePool* dst) {
    base::SpinLockHolder l(&lock_);
    base::SpinLockHolder l_them(&dst->lock_);

    dst->objects_.reserve(dst->objects_.size() + objects_.size());
    dst->objects_.insert(dst->objects_.end(), objects_.begin(), objects_.end());
    objects_.clear();
  }

 private:
  struct GenericElement {
    virtual ~GenericElement() {}
  };

  template <class T>
  struct SpecificElement : GenericElement {
    explicit SpecificElement(T *t): t(t) {}
    ~SpecificElement() {
      delete t;
    }

    T *t;
  };

  template <class T>
  struct SpecificArrayElement : GenericElement {
    explicit SpecificArrayElement(T *t): t(t) {}
    ~SpecificArrayElement() {
      delete [] t;
    }

    T *t;
  };

  typedef std::vector<GenericElement *> ElementVector;
  ElementVector objects_;
  base::SpinLock lock_;
};


} // namespace kudu
#endif
