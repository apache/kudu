// Copyright (c) 2013, Cloudera, inc.
//
// Simple pool of objects that will be deallocated when the pool is
// destroyed

#ifndef KUDU_UTIL_AUTO_RELEASE_POOL_H
#define KUDU_UTIL_AUTO_RELEASE_POOL_H

#include <vector>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>

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
    boost::lock_guard<boost::mutex> l(lock_);
    objects_.push_back(new SpecificElement<T>(t));
    return t;
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

  typedef std::vector<GenericElement *> ElementVector;
  ElementVector objects_;
  boost::mutex lock_;
};


} // namespace kudu
#endif
