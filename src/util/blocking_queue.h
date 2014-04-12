// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_BLOCKING_QUEUE_H
#define KUDU_UTIL_BLOCKING_QUEUE_H

#include <boost/foreach.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/type_traits/remove_pointer.hpp>
#include <unistd.h>
#include <list>
#include <tr1/type_traits>
#include <vector>

#include "gutil/basictypes.h"
#include "gutil/gscoped_ptr.h"

namespace kudu {

// Return values for BlockingQueue::Put()
enum QueueStatus {
  QUEUE_SUCCESS = 0,
  QUEUE_SHUTDOWN = 1,
  QUEUE_FULL = 2
};

template <typename T>
class BlockingQueue {
 public:
  // If T is a pointer, this will be the base type.  If T is not a pointer, you
  // can ignore this and the functions which make use of it.
  // Template substitution failure is not an error.
  typedef typename boost::remove_pointer<T>::type T_VAL;

  explicit BlockingQueue(size_t max_elements)
    : shutdown_(false),
      max_elements_(max_elements) {
  }

  // If the queue holds a bare pointer, it must be empty on destruction, since
  // it may have ownership of the pointer.
  ~BlockingQueue() {
    DCHECK(list_.empty() || !std::tr1::is_pointer<T>::value)
        << "BlockingQueue holds bare pointers at destruction time";
  }

  // Get an element from the queue.  Returns false if we were shut down prior to
  // getting the element.
  bool BlockingGet(T *out) {
    boost::unique_lock<boost::mutex> unique_lock(lock_);
    while (true) {
      if (!list_.empty()) {
        *out = list_.front();
        list_.pop_front();
        not_full_.notify_one();
        return true;
      }
      if (shutdown_) {
        return false;
      }
      not_empty_.wait(unique_lock);
    }
  }

  // Get an element from the queue.  Returns false if the queue is empty and
  // we were shut down prior to getting the element.
  bool BlockingGet(gscoped_ptr<T_VAL> *out) {
    T t;
    bool got_element = BlockingGet(&t);
    if (!got_element) {
      return false;
    }
    out->reset(t);
    return true;
  }

  // Get all elements from the queue and append them to a
  // vector. Returns false if shutdown prior to getting the elements.
  bool BlockingDrainTo(std::vector<T>* out) {
    boost::unique_lock<boost::mutex> unique_lock(lock_);
    while (true) {
      if (!list_.empty()) {
        out->reserve(list_.size());
        BOOST_FOREACH(const T& elt, list_) {
          out->push_back(elt);
        }
        list_.clear();
        not_full_.notify_one();
        return true;
      }
      if (shutdown_) {
        return false;
      }
      not_empty_.wait(unique_lock);
    }
  }

  // Attempts to put the given value in the queue.
  // Returns:
  //   QUEUE_SUCCESS: if successfully inserted
  //   QUEUE_FULL: if the queue has reached max_elements
  //   QUEUE_SHUTDOWN: if someone has already called Shutdown()
  QueueStatus Put(const T &val) {
    boost::lock_guard<boost::mutex> guard(lock_);
    if (list_.size() >= max_elements_) {
      return QUEUE_FULL;
    }
    if (shutdown_) {
      return QUEUE_SHUTDOWN;
    }
    list_.push_back(val);
    not_empty_.notify_one();
    return QUEUE_SUCCESS;
  }

  // Returns the same as the other Put() overload above.
  // If the element was inserted, the gscoped_ptr releases its contents.
  QueueStatus Put(gscoped_ptr<T_VAL> *val) {
    QueueStatus s = Put(val->get());
    if (s == QUEUE_SUCCESS) {
      ignore_result<>(val->release());
    }
    return s;
  }

  // Gets an element for the queue; if the queue is full, blocks until
  // space becomes available. Returns false if we were shutdown prior
  // to enqueueing the element.
  bool BlockingPut(const T& val) {
    boost::unique_lock<boost::mutex> unique_lock(lock_);
    while (true) {
      if (shutdown_) {
        return false;
      }
      if (list_.size() < max_elements_) {
        list_.push_back(val);
        not_empty_.notify_one();
        return true;
      }
      not_full_.wait(unique_lock);
    }
  }

  // Same as other BlockingPut() overload above. If the element was
  // enqueued, gscoped_ptr releases its contents.
  bool BlockingPut(gscoped_ptr<T_VAL>* val) {
    bool ret = Put(val->get());
    if (ret) {
      ignore_result(val->release());
    }
    return ret;
  }

  // Shut down the queue.
  // When a blocking queue is shut down, no more elements can be added to it,
  // and Put() will return QUEUE_SHUTDOWN.
  // Existing elements will drain out of it, and then BlockingGet will start
  // returning false.
  void Shutdown() {
    boost::lock_guard<boost::mutex> guard(lock_);
    shutdown_ = true;
    not_full_.notify_all();
    not_empty_.notify_all();
  }

  bool empty() const {
    boost::lock_guard<boost::mutex> guard(lock_);
    return list_.empty();
  }

  size_t max_elements() const {
    return max_elements_;
  }

 private:
  bool shutdown_;
  size_t max_elements_;
  boost::condition_variable not_empty_;
  boost::condition_variable not_full_;
  mutable boost::mutex lock_;
  std::list<T> list_;
};

} // namespace kudu

#endif
