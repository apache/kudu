// linked_ptr.h
// Copyright 2003 Google Inc.
//
// DEPRECATED(jyrki): linked_ptr is deprecated due to following reasons:
//   - linked_ptr is thread-hostile
//   - binary size bloat, memory size bloat
//
// Thread Safety:
//   A linked_ptr is thread-hostile. Copying a linked_ptr object is
//   effectively a read-write operation.
//
// Use the following instead:
//   - PointerVector or boost::ptr_container
//   - Explicit memory management (deleting the pointers using
//     STLDelete* or delete).
//   - See the C++ primer for the recommended uses of shared_ptr
//
// Original description:
//
// A "smart" pointer type with reference tracking.  Every pointer to a
// particular object is kept on a circular linked list.  When the last pointer
// to an object is destroyed or reassigned, the object is deleted.
//
// Used properly, this deletes the object when the last reference goes away.
// There are several caveats:
// - Like all reference counting schemes, cycles lead to leaks.
// - Each smart pointer is actually two pointers (8 bytes instead of 4).
// - Every time a pointer is released, the entire list of pointers to that
//   object is traversed.  This class is therefore NOT SUITABLE when there
//   will often be more than two or three pointers to a particular object.
// - References are only tracked as long as linked_ptr<> objects are copied.
//   If a linked_ptr<> is converted to a raw pointer and back, BAD THINGS
//   will happen (double deletion).
//
// A good use of this class is storing object references in STL containers.
// You can safely put linked_ptr<> in a vector<>.
// Other uses may not be as good.
//
// Note: If you use an incomplete type with linked_ptr<>, the class
// *containing* linked_ptr<> must have a constructor and destructor (even
// if they do nothing!).
//
// Bill Gibbons suggested we use something like this.  Yonat Sharon has
// a different (less useful IMHO) implementation at ootips.org.

#ifndef UTIL_GTL_LINKED_PTR_H_
#define UTIL_GTL_LINKED_PTR_H_

#include <stddef.h>

#include <glog/logging.h>
#include "gutil/logging-inl.h"  // for CHECK macros
#include "gutil/type_traits.h"  // for remove_pointer

// This is used internally by all instances of linked_ptr<>.  It needs to be
// a non-template class because different types of linked_ptr<> can refer to
// the same object (linked_ptr<Superclass>(obj) vs linked_ptr<Subclass>(obj)).
// So, it needs to be possible for different types of linked_ptr to participate
// in the same circular linked list, so we need a single class type here.
//
// DO NOT USE THIS CLASS DIRECTLY YOURSELF.  Use linked_ptr<T>.
class linked_ptr_internal {
 public:
  // Create a new circle that includes only this instance.
  void join_new() {
    next_ = this;
  }

  // Join an existing circle.
  void join(linked_ptr_internal const* ptr) {
    next_ = ptr->next_;
    ptr->next_ = this;
  }

  // Leave whatever circle we're part of.  Returns true iff we were the
  // last member of the circle.  Once this is done, you can join() another.
  bool depart() {
    if (next_ == this) return true;
    linked_ptr_internal const* p = next_;
    while (p->next_ != this) p = p->next_;
    p->next_ = next_;
    return false;
  }

 private:
  mutable linked_ptr_internal const* next_;
};

template <typename T>
class linked_ptr {
 public:
  typedef T element_type;

  // Take over ownership of a raw pointer.  This should happen as soon as
  // possible after the object is created.
  explicit linked_ptr(T* ptr = NULL) { capture(ptr); }
  ~linked_ptr() { depart(); }

  // Copy an existing linked_ptr<>, adding ourselves to the list of references.
  // TODO(user): explicit?
  template <typename U> linked_ptr(linked_ptr<U> const& ptr) { copy(&ptr); }
  linked_ptr(linked_ptr const& ptr) { DCHECK_NE(&ptr, this); copy(&ptr); }

  // Assignment releases the old value and acquires the new.
  template <typename U> linked_ptr& operator=(linked_ptr<U> const& ptr) {
    depart();
    copy(&ptr);
    return *this;
  }

  linked_ptr& operator=(linked_ptr const& ptr) {
    if (&ptr != this) {
      depart();
      copy(&ptr);
    }
    return *this;
  }

  // Smart pointer members.
  void reset(T* ptr = NULL) { depart(); capture(ptr); }
  T* get() const { return value_; }
  T* operator->() const { return value_; }
  T& operator*() const { return *value_; }
  // Release ownership of the pointed object and returns it.
  // Sole ownership by this linked_ptr object is required.
  T* release() {
    bool last = link_.depart();
    CHECK(last);
    T* v = value_;
    value_ = NULL;
    return v;
  }

  bool operator==(T* p) const { return value_ == p; }
  bool operator!=(T* p) const { return value_ != p; }
  bool operator<(T* p) const { return value_ < p; }
  bool operator<=(T* p) const { return value_ <= p; }
  bool operator>(T* p) const { return value_ > p; }
  bool operator>=(T* p) const { return value_ >= p; }
  template <typename U>
  bool operator==(linked_ptr<U> const& ptr) const {
    return value_ == ptr.get();
  }
  template <typename U>
  bool operator!=(linked_ptr<U> const& ptr) const {
    return value_ != ptr.get();
  }
  template <typename U>
  bool operator<(linked_ptr<U> const& ptr) const {
    return value_ < ptr.get();
  }
  template <typename U>
  bool operator<=(linked_ptr<U> const& ptr) const {
    return value_ <= ptr.get();
  }
  template <typename U>
  bool operator>(linked_ptr<U> const& ptr) const {
    return value_ > ptr.get();
  }
  template <typename U>
  bool operator>=(linked_ptr<U> const& ptr) const {
    return value_ >= ptr.get();
  }

 private:
  template <typename U>
  friend class linked_ptr;

  T* value_;
  linked_ptr_internal link_;

  void depart() {
    if (link_.depart()) delete value_;
  }

  void capture(T* ptr) {
    value_ = ptr;
    link_.join_new();
  }

  template <typename U> void copy(linked_ptr<U> const* ptr) {
    value_ = ptr->get();
    if (value_)
      link_.join(&ptr->link_);
    else
      link_.join_new();
  }
};

template<typename T> inline
bool operator==(T* ptr, const linked_ptr<T>& x) {
  return ptr == x.get();
}

template<typename T> inline
bool operator!=(T* ptr, const linked_ptr<T>& x) {
  return ptr != x.get();
}

template<typename T> inline
bool operator<(T* ptr, const linked_ptr<T>& x) {
  return ptr < x.get();
}

template<typename T> inline
bool operator<=(T* ptr, const linked_ptr<T>& x) {
  return ptr <= x.get();
}

template<typename T> inline
bool operator>(T* ptr, const linked_ptr<T>& x) {
  return ptr > x.get();
}

template<typename T> inline
bool operator>=(T* ptr, const linked_ptr<T>& x) {
  return ptr >= x.get();
}

// A function to convert T* into linked_ptr<T>
// Doing e.g. make_linked_ptr(new FooBarBaz<type>(arg)) is a shorter notation
// for linked_ptr<FooBarBaz<type> >(new FooBarBaz<type>(arg))
template <typename T>
linked_ptr<T> make_linked_ptr(T* ptr) {
  return linked_ptr<T>(ptr);
}

namespace base {
template<typename T> struct remove_pointer<linked_ptr<T> > { typedef T type; };
}  // namespace base

#endif  // UTIL_GTL_LINKED_PTR_H_
