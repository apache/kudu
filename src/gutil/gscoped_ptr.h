// Copyright 2007 Google Inc.
// All Rights Reserved.
//
//
#ifndef BASE_SCOPED_PTR_H__
#define BASE_SCOPED_PTR_H__

//  This implementation was designed to match the then-anticipated TR2
//  implementation of the scoped_ptr class, and its closely-related brethren,
//  scoped_array, scoped_ptr_malloc, and make_scoped_ptr. The anticipated
//  standardization of scoped_ptr has been superseded by unique_ptr, and
//  the APIs in this file are being revised to be a subset of unique_ptr).
//
//  Kudu notes: this should be used in preference to boost::scoped_ptr since
//  it offers a ::release() method like unique_ptr. We unfortunately cannot
//  just use unique_ptr because it has an inconsistent implementation in
//  some of the older compilers we have to support.

#include <assert.h>
#include <stdlib.h>
#include <cstddef>

#include "gutil/gscoped_ptr_internals.h"

#ifdef OS_EMBEDDED_QNX
// NOTE(user):
// The C++ standard says that <stdlib.h> declares both ::foo and std::foo
// But this isn't done in QNX version 6.3.2 200709062316.
using std::free;
using std::malloc;
using std::realloc;
#endif

template <class C, class D> class gscoped_ptr;
template <class C, class Free> class gscoped_ptr_malloc;
template <class C> class gscoped_array;

namespace base {

// Function object which deletes its parameter, which must be a pointer.
// If C is an array type, invokes 'delete[]' on the parameter; otherwise,
// invokes 'delete'. The default deleter for gscoped_ptr<T>.
template <class C>
struct DefaultDeleter {
  inline void operator()(C* ptr) const {
    enum { type_must_be_complete = sizeof(C) };
    delete ptr;
  }
};

// Specialization of DefaultDeleter for array types.
template <class C>
struct DefaultDeleter<C[]> {
  inline void operator()(C* ptr) const {
    enum { type_must_be_complete = sizeof(C) };
    delete[] ptr;
  }
};

// Function object which invokes 'free' on its parameter, which must be
// a pointer. Can be used to store malloc-allocated pointers in gscoped_ptr:
//
// gscoped_ptr<int, base::FreeDeleter> foo_ptr(
//     static_cast<int>(malloc(sizeof(int))));
struct FreeDeleter {
  inline void operator()(void* ptr) const {
    free(ptr);
  }
};

}  // namespace base

template <class C>
gscoped_ptr<C, base::DefaultDeleter<C> > make_gscoped_ptr(C *);

// A gscoped_ptr<T> is like a T*, except that the destructor of gscoped_ptr<T>
// automatically deletes the pointer it holds (if any).
// That is, gscoped_ptr<T> owns the T object that it points to.
// Like a T*, a gscoped_ptr<T> may hold either NULL or a pointer to a T object.
// Also like T*, gscoped_ptr<T> is thread-compatible, and once you
// dereference it, you get the threadsafety guarantees of T.
//
// By default, gscoped_ptr deletes its stored pointer using 'delete', but
// this behavior can be customized via the second template parameter:
// A gscoped_ptr<T,D> invokes D::operator() on the stored pointer when the
// gscoped_ptr is destroyed. For example, a gscoped_ptr<T, base::FreeDeleter>
// can be used to store pointers to memory allocated with malloc().
// Note that gscoped_ptr will not invoke D on a NULL pointer.
//
// If D is an empty class (i.e. has no non-static data members), then
// on most compilers, gscoped_ptr is the same size as a plain pointer.
// Otherwise, it will be at least as large as sizeof(C*) + sizeof(D).
template <class C, class D = base::DefaultDeleter<C> >
class gscoped_ptr {
 public:

  // The element type
  typedef C element_type;
  typedef D deleter_type;

  // Constructor.  Defaults to intializing with NULL.
  // There is no way to create an uninitialized gscoped_ptr.
  explicit gscoped_ptr(C* p = NULL) : impl_(p) { }

  // Reset.  Deletes the current owned object, if any.
  // Then takes ownership of a new object, if given.
  // this->reset(this->get()) works, but this behavior is DEPRECATED, and
  void reset(C* p = NULL) {
    impl_.reset(p);
  }

  // Accessors to get the owned object.
  // operator* and operator-> will assert() if there is no current object.
  C& operator*() const {
    assert(impl_.get() != NULL);
    return *impl_.get();
  }
  C* operator->() const  {
    assert(impl_.get() != NULL);
    return impl_.get();
  }
  C* get() const { return impl_.get(); }

  // Comparison operators.
  // These return whether a gscoped_ptr and a raw pointer refer to
  // the same object, not just to two different but equal objects.
  bool operator==(const C* p) const { return impl_.get() == p; }
  bool operator!=(const C* p) const { return impl_.get() != p; }

  // Swap two scoped pointers.
  void swap(gscoped_ptr& p2) {
    impl_.swap(p2.impl_);
  }

  // Release a pointer.
  // The return value is the current pointer held by this object.
  // If this object holds a NULL pointer, the return value is NULL.
  // After this operation, this object will hold a NULL pointer,
  // and will not own the object any more.
  //
  // CAVEAT: It is incorrect to use and release a pointer in one statement, eg.
  //   objects[ptr->name()] = ptr.release();
  // as it is undefined whether the .release() or ->name() runs first.
  C* release() {
    return impl_.release();
  }

 private:
  base::internal::gscoped_ptr_impl<C,D> impl_;

  // calls a copy ctor, there will be a problem) see below
  friend gscoped_ptr<C, base::DefaultDeleter<C> > make_gscoped_ptr<C>(C *p);

  // Forbid comparison of gscoped_ptr types.  If C2 != C, it totally doesn't
  // make sense, and if C2 == C, it still doesn't make sense because you should
  // never have the same object owned by two different gscoped_ptrs.
  template <class C2, class D2> bool operator==(
      gscoped_ptr<C2, D2> const& p2) const;
  template <class C2, class D2> bool operator!=(
      gscoped_ptr<C2, D2> const& p2) const;

  // Disallow copy and assignment.
  gscoped_ptr(const gscoped_ptr&);
  void operator=(const gscoped_ptr&);
};

// Free functions
template <class C, class D>
inline void swap(gscoped_ptr<C, D>& p1, gscoped_ptr<C, D>& p2) {
  p1.swap(p2);
}

template <class C, class D>
inline bool operator==(const C* p1, const gscoped_ptr<C, D>& p2) {
  return p1 == p2.get();
}

template <class C, class D>
inline bool operator==(const C* p1, const gscoped_ptr<const C, D>& p2) {
  return p1 == p2.get();
}

template <class C, class D>
inline bool operator!=(const C* p1, const gscoped_ptr<C, D>& p2) {
  return p1 != p2.get();
}

template <class C, class D>
inline bool operator!=(const C* p1, const gscoped_ptr<const C, D>& p2) {
  return p1 != p2.get();
}

// Specialization of gscoped_ptr used for holding arrays:
//
// gscoped_ptr<int[]> array(new int[10]);
//
// This specialization provides operator[] instead of operator* and
// operator->, and by default it deletes the stored array using 'delete[]'
// rather than 'delete'. It also provides some additional type-safety:
// the pointer used to initialize a gscoped_ptr<T[]> must have type T* and
// not, for example, some class derived from T; this helps avoid
// accessing an array through a pointer whose dynamic type is different
// from its static type, which can lead to undefined behavior.
template <class C, class D>
class gscoped_ptr<C[], D> {
 public:

  // The element type
  typedef C element_type;
  typedef D deleter_type;

  // Default constructor. Initializes stored pointer to NULL.
  // There is no way to create an uninitialized gscoped_ptr.
  gscoped_ptr() : impl_(NULL) { }

  // Constructor. Stores the given array. Note that the argument's type
  // must exactly match C*. In particular:
  // - it cannot be a pointer to a type derived from C, because it is
  //   inherently unsafe to access an array through a pointer whose
  //   dynamic type does not match its static type. If you're doing this,
  //   fix your code.
  // - it cannot be NULL, because NULL is an integral expression, not a
  //   pointer to C. Use the no-argument version instead of explicitly
  //   passing NULL.
  // - it cannot be const-qualified differently from C. You can work around
  //   this using implicit_cast (from base/casts.h):
  //
  //   int* i;
  //   gscoped_ptr<const int[]> arr(implicit_cast<const int[]>(i));
  explicit gscoped_ptr(C* array) : impl_(array) { }

  // Reset.  Deletes the current owned object, if any, then takes ownership of
  // the new object, if given. Note that the argument's type must exactly
  // match C*; see the comments on the constructor for details.
  // this->reset(this->get()) works, but this behavior is DEPRECATED, and
  void reset(C* array) {
    impl_.reset(array);
  }

  void reset() {
    // Cast is necessary to avoid matching disabled reset() template below.
    reset(static_cast<C*>(NULL));
  }

  // Array indexing operation. Returns the specified element of the underlying
  // array. Will assert if no array is currently stored.
  C& operator[] (size_t i) const {
    assert(impl_.get() != NULL);
    return impl_.get()[i];
  }

  C* get() const { return impl_.get(); }

  // Comparison operators.
  // These return whether a gscoped_ptr and a raw pointer refer to
  // the same object, not just to two different but equal objects.
  bool operator==(const C* array) const { return impl_.get() == array; }
  bool operator!=(const C* array) const { return impl_.get() != array; }

  // Swap two scoped pointers.
  void swap(gscoped_ptr& p2) {
    impl_.swap(p2.impl_);
  }

  // Release a pointer.
  // The return value is a pointer to the array currently held by this object.
  // If this object holds a NULL pointer, the return value is NULL.
  // After this operation, this object will hold a NULL pointer,
  // and will not own the array any more.
  //
  // CAVEAT: It is incorrect to use and release a pointer in one statement, eg.
  //   objects[ptr->name()] = ptr.release();
  // as it is undefined whether the .release() or ->name() runs first.
  C* release() {
    return impl_.release();
  }

 private:
  // Force C to be a complete type.
  enum { type_must_be_complete = sizeof(C) };

  // Disable initialization from any type other than C*, by providing a
  // constructor that matches such an initialization, but is private and
  // has no definition. This is disabled because it is not safe to
  // call delete[] on an array whose static type does not match its dynamic
  // type.
  template <typename T>
  explicit gscoped_ptr(T* array);

  // Disable reset() from any type other than C*, for the same reasons
  // as the constructor above.
  template <typename T>
  void reset(T* array);

  base::internal::gscoped_ptr_impl<C,D> impl_;

  // Forbid comparison of gscoped_ptr types.  If C2 != C, it totally doesn't
  // make sense, and if C2 == C, it still doesn't make sense because you should
  // never have the same object owned by two different gscoped_ptrs.
  template <class C2, class D2> bool operator==(
      gscoped_ptr<C2, D2> const& p2) const;
  template <class C2, class D2> bool operator!=(
      gscoped_ptr<C2, D2> const& p2) const;

  // Disallow copy and assignment.
  gscoped_ptr(const gscoped_ptr&);
  void operator=(const gscoped_ptr&);
};

template <class C, class D>
inline bool operator==(const C* p1, const gscoped_ptr<C[], D>& p2) {
  return p1 == p2.get();
}

template <class C, class D>
inline bool operator==(const C* p1, const gscoped_ptr<const C[], D>& p2) {
  return p1 == p2.get();
}

template <class C, class D>
inline bool operator!=(const C* p1, const gscoped_ptr<C[], D>& p2) {
  return p1 != p2.get();
}

template <class C, class D>
inline bool operator!=(const C* p1, const gscoped_ptr<const C[], D>& p2) {
  return p1 != p2.get();
}

template <class C>
gscoped_ptr<C> make_gscoped_ptr(C *p) {
  // This does nothing but to return a gscoped_ptr of the type that the passed
  // pointer is of.  (This eliminates the need to specify the name of T when
  // making a gscoped_ptr that is used anonymously/temporarily.)  From an
  // access control point of view, we construct an unnamed gscoped_ptr here
  // which we return and thus copy-construct.  Hence, we need to have access
  // to gscoped_ptr::gscoped_ptr(gscoped_ptr const &).  However, it is guaranteed
  // that we never actually call the copy constructor, which is a good thing
  // as we would call the temporary's object destructor (and thus delete p)
  // if we actually did copy some object, here.
  return gscoped_ptr<C>(p);
}

// gscoped_array<C> is like gscoped_ptr<C>, except that the caller must allocate
// with new [] and the destructor deletes objects with delete [].
//
// As with gscoped_ptr<C>, a gscoped_array<C> either points to an object
// or is NULL.  A gscoped_array<C> owns the object that it points to.
// gscoped_array<T> is thread-compatible, and once you index into it,
// the returned objects have only the threadsafety guarantees of T.
//
// Size: sizeof(gscoped_array<C>) == sizeof(C*)
//
// aware of the following differences:
// - The pointers passed into gscoped_ptr<C[]> must have type C* exactly.
//   See the comments on gscoped_ptr<C[]>'s constructor for details.
// - The type C must be complete (i.e. it must have a full definition, not
//   just a forward declaration) at the point where the gscoped_ptr<C[]> is
//   declared.
template <class C>
class gscoped_array {
 public:

  // The element type
  typedef C element_type;

  // Constructor.  Defaults to intializing with NULL.
  // There is no way to create an uninitialized gscoped_array.
  // The input parameter must be allocated with new [].
  explicit gscoped_array(C* p = NULL) : array_(p) { }

  // Destructor.  If there is a C object, delete it.
  // We don't need to test ptr_ == NULL because C++ does that for us.
  ~gscoped_array() {
    enum { type_must_be_complete = sizeof(C) };
    delete[] array_;
  }

  // Reset.  Deletes the current owned object, if any.
  // Then takes ownership of a new object, if given.
  // this->reset(this->get()) works.
  void reset(C* p = NULL) {
    if (p != array_) {
      enum { type_must_be_complete = sizeof(C) };
      delete[] array_;
      array_ = p;
    }
  }

  // Get one element of the current object.
  // Will assert() if there is no current object, or index i is negative.
  C& operator[](std::ptrdiff_t i) const {
    assert(i >= 0);
    assert(array_ != NULL);
    return array_[i];
  }

  // Get a pointer to the zeroth element of the current object.
  // If there is no current object, return NULL.
  C* get() const {
    return array_;
  }

  // Comparison operators.
  // These return whether a gscoped_array and a raw pointer refer to
  // the same array, not just to two different but equal arrays.
  bool operator==(const C* p) const { return array_ == p; }
  bool operator!=(const C* p) const { return array_ != p; }

  // Swap two scoped arrays.
  void swap(gscoped_array& p2) {
    C* tmp = array_;
    array_ = p2.array_;
    p2.array_ = tmp;
  }

  // Release an array.
  // The return value is the current pointer held by this object.
  // If this object holds a NULL pointer, the return value is NULL.
  // After this operation, this object will hold a NULL pointer,
  // and will not own the object any more.
  C* release() {
    C* retVal = array_;
    array_ = NULL;
    return retVal;
  }

 private:
  C* array_;

  // Forbid comparison of different gscoped_array types.
  template <class C2> bool operator==(gscoped_array<C2> const& p2) const;
  template <class C2> bool operator!=(gscoped_array<C2> const& p2) const;

  // Disallow copy and assignment.
  gscoped_array(const gscoped_array&);
  void operator=(const gscoped_array&);
};

// Free functions
template <class C>
inline void swap(gscoped_array<C>& p1, gscoped_array<C>& p2) {
  p1.swap(p2);
}

template <class C>
inline bool operator==(const C* p1, const gscoped_array<C>& p2) {
  return p1 == p2.get();
}

template <class C>
inline bool operator==(const C* p1, const gscoped_array<const C>& p2) {
  return p1 == p2.get();
}

template <class C>
inline bool operator!=(const C* p1, const gscoped_array<C>& p2) {
  return p1 != p2.get();
}

template <class C>
inline bool operator!=(const C* p1, const gscoped_array<const C>& p2) {
  return p1 != p2.get();
}

// gscoped_ptr_malloc<> is similar to gscoped_ptr<>, but it accepts a
// second template argument, the functor used to free the object.
//
// gscoped_ptr has a slightly different API, will not invoke the deleter
// on NULL pointers, and maintains a separate deleter object for each
// gscoped_ptr, rather than a single static deleter object shared across
// all instances as gscoped_ptr_malloc does.
template<class C, class FreeProc = base::FreeDeleter>
class gscoped_ptr_malloc {
 public:

  // The element type
  typedef C element_type;

  // Construction with no arguments sets ptr_ to NULL.
  // There is no way to create an uninitialized gscoped_ptr.
  // The input parameter must be allocated with an allocator that matches the
  // Free functor.  For the default Free functor, this is malloc, calloc, or
  // realloc.
  explicit gscoped_ptr_malloc(): ptr_(NULL) { }

  // Construct with a C*, and provides an error with a D*.
  template<class must_be_C>
  explicit gscoped_ptr_malloc(must_be_C* p): ptr_(p) { }

  // Construct with a void*, such as you get from malloc.
  explicit gscoped_ptr_malloc(void *p): ptr_(static_cast<C*>(p)) { }

  // Destructor.  If there is a C object, call the Free functor.
  ~gscoped_ptr_malloc() {
    free_(ptr_);
  }

  // Reset.  Calls the Free functor on the current owned object, if any.
  // Then takes ownership of a new object, if given.
  // this->reset(this->get()) works.
  void reset(C* p = NULL) {
    if (ptr_ != p) {
      free_(ptr_);
      ptr_ = p;
    }
  }

  // Reallocates the existing pointer, and returns 'true' if
  // the reallcation is succesfull.  If the reallocation failed, then
  // the pointer remains in its previous state.
  //
  // Note: this calls realloc() directly, even if an alternate 'free'
  // functor is provided in the template instantiation.
  bool try_realloc(size_t new_size) {
    C* new_ptr = static_cast<C*>(realloc(ptr_, new_size));
    if (new_ptr == NULL) {
      return false;
    }
    ptr_ = new_ptr;
    return true;
  }

  // Get the current object.
  // operator* and operator-> will cause an assert() failure if there is
  // no current object.
  C& operator*() const {
    assert(ptr_ != NULL);
    return *ptr_;
  }

  C* operator->() const {
    assert(ptr_ != NULL);
    return ptr_;
  }

  C* get() const {
    return ptr_;
  }

  // Comparison operators.
  // These return whether a gscoped_ptr_malloc and a plain pointer refer
  // to the same object, not just to two different but equal objects.
  // For compatibility with the boost-derived implementation, these
  // take non-const arguments.
  bool operator==(C* p) const {
    return ptr_ == p;
  }

  bool operator!=(C* p) const {
    return ptr_ != p;
  }

  // Swap two scoped pointers.
  void swap(gscoped_ptr_malloc & b) {
    C* tmp = b.ptr_;
    b.ptr_ = ptr_;
    ptr_ = tmp;
  }

  // Release a pointer.
  // The return value is the current pointer held by this object.
  // If this object holds a NULL pointer, the return value is NULL.
  // After this operation, this object will hold a NULL pointer,
  // and will not own the object any more.
  C* release() {
    C* tmp = ptr_;
    ptr_ = NULL;
    return tmp;
  }

 private:
  C* ptr_;

  // no reason to use these: each gscoped_ptr_malloc should have its own object
  template <class C2, class GP>
  bool operator==(gscoped_ptr_malloc<C2, GP> const& p) const;
  template <class C2, class GP>
  bool operator!=(gscoped_ptr_malloc<C2, GP> const& p) const;

  static FreeProc const free_;

  // Disallow copy and assignment.
  gscoped_ptr_malloc(const gscoped_ptr_malloc&);
  void operator=(const gscoped_ptr_malloc&);
};

template<class C, class FP>
FP const gscoped_ptr_malloc<C, FP>::free_ = FP();

template<class C, class FP> inline
void swap(gscoped_ptr_malloc<C, FP>& a, gscoped_ptr_malloc<C, FP>& b) {
  a.swap(b);
}

template<class C, class FP> inline
bool operator==(C* p, const gscoped_ptr_malloc<C, FP>& b) {
  return p == b.get();
}

template<class C, class FP> inline
bool operator!=(C* p, const gscoped_ptr_malloc<C, FP>& b) {
  return p != b.get();
}

#endif  // BASE_GSCOPED_PTR_H__
