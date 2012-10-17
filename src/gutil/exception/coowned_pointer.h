// Copyright 2011 Google Inc.  All Rights Reserved
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
//
//
// Defines a helper 'smart pointer' class used in exception propagation. It's
// similar to linked_ptr, but w/ different release semantics: releasing does
// not set the value to NULL, and does not require that the argument is the
// last member of the linked list. Calling release() on any co-owner revokes
// ownership from all of them, transferring it to the caller. Nonetheless, after
// the release, get() continues to return the original pointer. (It is the
// responsibility of the caller of get() to ensure that the object, if
// it has been released, is still alive).

#ifndef DATAWAREHOUSE_COMMON_EXCEPTION_COOWNED_POINTER_H_
#define DATAWAREHOUSE_COMMON_EXCEPTION_COOWNED_POINTER_H_

#include <glog/logging.h>
#include "gutil/logging-inl.h"  // for CHECK macros

namespace common {

template<typename T>
class CoownedPointer {
 public:
  // Creates a pointer to NULL.
  explicit CoownedPointer() : value_(NULL), next_(NULL) {}

  // If the value is not NULL, creates a pointer that is a sole owner of the
  // value. Otherwise, creates a pointer to NULL.
  explicit CoownedPointer(T* value)
      : value_(value),
        next_(value != NULL ? this : NULL) {}

  // Makes this a copy of the other.
  // If other.get() == NULL, creates a pointer to NULL. Otherwise, creates a
  // pointer to the other's value, that will be a co-owner iff the other is
  // an owner (i.e. iff the value has not been released).
  CoownedPointer(const CoownedPointer& other) : value_(other.value_) {
    join(other);
  }

  // Destroys the pointer. Deletes the value if we're an owner (i.e. if not
  // released) and if we're the last owner alive.
  ~CoownedPointer() {
    if (depart()) delete value_;
  }

  // Returns a pointer to the value, without transferring ownership.
  // Continues to return the value even after release(). If !this->is_owner(),
  // it is the responsibility of the caller to ensure that the value is alive.
  T* get() { return value_; }

  // Returns a const pointer to the value, without transferring ownership.
  // Continues to return the value even after release(). If !this->is_owner(),
  // it is the responsibility of the caller to ensure that the value is alive.
  const T* get() const { return value_; }

  // Makes this point to the other's object, and co-own it if it has not been
  // released.
  CoownedPointer& operator=(const CoownedPointer& other) {
    if (&other != this) {
      if (depart()) delete value_;
      value_ = other.value_;
      join(other);
    }
    return *this;
  }

  // Returns true if the referenced object is not NULL and has not yet been
  // released. If (!is_owner() && get() != NULL), then calling release() is
  // prohibited (as the object has already been released).
  bool is_owner() const { return next_ != NULL; }

  // If the referenced object is NULL, this method has no effect and returns
  // NULL. Otherwise, it returns the referenced object, passing its ownership
  // to the caller.
  // After this method is called with a non-NULL referenced object:
  //   * is_owner() will return false for this and all peers.
  //   * get() will continue to return the referenced object.
  //   * deleting this and peers will not destroy the referenced object.
  //   * calling release() again, on this on any peers, is illegal (will cause
  //     crash).
  T* release() {
    const CoownedPointer* p = next_;
    if (p == NULL) {
      CHECK(value_ == NULL) << "Release called on a released pointer.";
    } else {
      do {
        const CoownedPointer* prev = p;
        p = p->next_;
        prev->next_ = NULL;
      } while (p != NULL);
    }
    return value_;
  }

 private:
  // If (other.is_owner()), joins the circle of co-owners. Otherwise, makes
  // this a non-owner.
  void join(const CoownedPointer& other) {
    next_ = other.next_;
    if (other.next_ != NULL) other.next_ = this;
  }
  // If (!is_owner()), i.e. NULL or released, returns false. Otherwise, if this
  // is the sole owner, returns true. Otherwise, removes this from the
  // co-owner's list and returns false.
  // (Returning true indicates that the value should be deleted).
  bool depart() {
    if (next_ == NULL) return false;  // Released already, or the value is NULL.
    if (next_ == this) return true;   // We're the last man standing.
    const CoownedPointer* p = next_;
    while (p->next_ != this) p = p->next_;
    p->next_ = next_;
    return false;
  }

  // Pointer to the referenced object.
  T* value_;

  // Circular linked list.
  // When value_ == NULL, next_ is always NULL.
  // When value_ != NULL, next_ is set to NULL by release().
  mutable CoownedPointer* next_;
  // Copyable.
};

}  // namespace

#endif  // DATAWAREHOUSE_COMMON_EXCEPTION_COOWNED_POINTER_H_
