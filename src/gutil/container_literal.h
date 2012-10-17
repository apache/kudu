// Copyright 2009 Google Inc. All Rights Reserved.

// Utilities for simulating container literals in C++03 (C++11's
// initializer list feature makes them largely useless).  You can use
// them to create and populate an STL-style container in a single
// expression:
//
//   util::gtl::Container(e1, ..., en)
//
// creates a container of any type with the given n elements, as long
// as the desired type has a constructor that takes a pair of
// iterators (begin, end).
//
//   util::gtl::NewContainer(e1, ..., en)
//
// is similar, except that the container is created on the heap and a
// pointer to it is returned.
//
// These constructs can also be used to create associative containers.
// For example,
//
//   hash_map<string, int> word_lengths =
//       util::gtl::Container(make_pair("hi", 2), make_pair("hello", 5));
//
// results in word_lengths containing two entries:
//
//   word_lengths["hi"] == 2 and word_lengths["hello"] == 5.
//
// The syntax is designed to mimic C++11's initializer list.  For
// example, given a function Foo() that takes a const vector<int>&, in
// C++11 you can write:
//
//   Foo({ 1, 3, 7 });
//
// while this header allows you to write:
//
//   Foo(util::gtl::Container(1, 3, 7));
//
// All values passed to Container() or NewContainer() must have the
// same type.  If the compiler infers different types for them, you'll
// need to disambiguate:
//
//   list<double> foo = util::gtl::Container<double>(1, 3.5, 7);
//
// Currently we support up-to 40 elements in the container.  If this
// is inadequate, you can easily raise the limit by increasing the
// value of max_arity in container_literal_generated.h.pump and
// re-generating container_literal_generated.h.
//
// CAVEATS:
//
// 1. When you write
//
//   T array[] = { e1, ..., en };
//
// the elements are guaranteed to be evaluated in the order they
// appear.  This is NOT the case for Container() and NewContainer(),
// as C++ doesn't guarantee the evaluation order of function
// arguments.  Therefore avoid side effects in e1, ..., and en.
//
// 2. Unfortunately, since many STL container classes have multiple
// single-argument constructors, the following syntaxes don't compile
// in C++98 mode:
//
//   vector<int> foo(util::gtl::Container(1, 5, 3));
//   static_cast<vector<int> >(util::gtl::Container(3, 4));
//
// (Note that NewContainer() doesn't suffer from this limitation.)
//
// The latter is especially a problem since it makes it hard to call
// functions overloaded with different container types.  Therefore,
// Container() provides an As<DesiredContainer>() method for
// explicitly specifying the desired container type.  For example:
//
//   // Calls Foo(const vector<int>&).
//   Foo(util::gtl::Container(2, 3).As<vector<int> >());
//   // Calls Foo(const list<int64>&).
//   Foo(util::gtl::Container(4).As<list<int64> >());
//
// 3. For efficiency, Container(...) and NewContainer(...) do NOT make
// a copy of the elements before adding them to the target container -
// it needs the original elements to be around when being converted to
// the target container type.  Therefore make sure the conversion is
// done while the original elements are still alive.

#ifndef UTIL_GTL_CONTAINER_LITERAL_H_
#define UTIL_GTL_CONTAINER_LITERAL_H_

#include <string.h>
#include <cstddef>
#include <iterator>
using std::back_insert_iterator;
using std::iterator_traits;
#include <string>
using std::string;

#include "gutil/port.h"
#include "gutil/type_traits.h"

// Must come after "base/port.h".
#ifdef LANG_CXX11
// Allow uses of C++11 features in this header. DO NOT DO THIS IN YOUR OWN CODE
// for the following reasons:
//  1) C++11 features may have hidden pitfalls, so c-style wants to introduce
//     them gradually, while this pragma allows all of them at once
//  2) This pragma turns off *all* warnings in this file, so you're more likely
//     to write broken code if you use it.
#pragma GCC system_header
#include <initializer_list>
#endif

namespace util {
namespace gtl {
namespace internal {

// Internal utilities for implementing Container() and NewContainer().
// They are subject to change without notice.  Therefore DO NOT USE
// THEM DIRECTLY.

// A DerefIterator<T> object can walk through an array of T pointers
// and retrieve the T objects they point to.
template <typename T>
class DerefIterator {
 public:
  // These typedefs are required by STL algorithms using an iterator.
  typedef std::forward_iterator_tag iterator_category;
  typedef typename base::remove_const<T>::type value_type;
  typedef T* pointer;
  typedef T& reference;
  typedef ptrdiff_t difference_type;

  explicit DerefIterator(T** ptr) : ptr_(ptr) {}

  // The compiler-generated copy constructor and assignment operator
  // are exactly what we need, so we don't define our own.

  // ++it.
  DerefIterator& operator++() {
    ++ptr_;
    return *this;
  }

  // it++.
  DerefIterator operator++(int /* dummy */) {
    const DerefIterator old(*this);
    ++ptr_;
    return old;
  }

  reference operator*() const { return **ptr_; }
  pointer operator->() const { return *ptr_; }
  bool operator==(const DerefIterator& rhs) const { return ptr_ == rhs.ptr_; }
  bool operator!=(const DerefIterator& rhs) const { return ptr_ != rhs.ptr_; }

 private:
  T** ptr_;
};

#ifdef LANG_CXX11

// IsNotInitializerList<T>::type exists iff T is not an initializer_list.
template <typename T>
struct IsNotInitializerList {
  typedef void type;
};
template <typename T>
struct IsNotInitializerList<std::initializer_list<T> > {};

#endif

// ContainerImpl<T, kCount> implements a collection of kCount values
// whose type is T.  It can be converted to any STL-style container
// that has a constructor taking an iterator range.
//
// For efficiency, a ContainerImpl<T, kCount> object does not make a
// copy of the values - it saves pointers to them instead.  Therefore
// it must be converted to an STL-style container while the values are
// alive.  We guarantee this by declaring the type ContainerImpl<T,
// kCount> internal and forbidding people to use it directly: since a
// user is not allowed to save an object of this type to a variable
// and use it later, we don't need to worry about the source values
// dying too soon.
template <typename T, size_t kCount>
class ContainerImpl {
 public:
  // elem_ptrs is an array of kCount pointers.
  explicit ContainerImpl(const T* elem_ptrs[]) {
    memcpy(elem_ptrs_, elem_ptrs, kCount*sizeof(*elem_ptrs));
  }

  // The compiler-generated copy constructor does exactly what we
  // want, so we don't define our own.  The copy constructor is needed
  // as the Container() functions need to return a ContainerImpl
  // object by value.

  // Converts the collection to the desired container type.
  // We aren't able to merge the two #ifdef branches, as default
  // function template argument is a feature new in C++0X.
#ifdef LANG_CXX11
  // We use SFINAE to restrict conversion to container-like types (by
  // testing for the presence of a const_iterator member type) and
  // also to disable conversion to an initializer_list (which also
  // has a const_iterator).  Otherwise code like
  //   vector<int> foo;
  //   foo = Container(1, 2, 3);
  // or
  //   typedef map<int, vector<string> > MapByAge;
  //   MapByAge m;
  //   m.insert(MapByAge::value_type(21, Container("J. Dennett", "A. Liar")));
  // will fail to compile in C++11 due to ambiguous conversion paths
  // (in C++11 vector<T>::operator= is overloaded to take either a
  // vector<T> or an initializer_list<T>, and pair<A, B> has a templated
  // constructor).
  template <typename DesiredType,
            typename IsNotInitializerListChecker=
                typename IsNotInitializerList<DesiredType>::type,
            typename ContainerChecker=
                typename DesiredType::const_iterator
            >
#else
  template <typename DesiredType>
#endif
  operator DesiredType() {
    return DesiredType(DerefIterator<const T>(elem_ptrs_),
                       DerefIterator<const T>(elem_ptrs_ + kCount));
  }

  template <typename DesiredContainer>
  DesiredContainer As() {
    return this->operator DesiredContainer();
  }

 private:
  const T* elem_ptrs_[kCount];

  // We don't need an assignment operator.
  void operator=(const ContainerImpl&);
};

class ContainerImpl0 {
 public:
#ifdef LANG_CXX11
  // Use SFINAE to restrict conversion to container-like targets (except
  // initializer_list).
  template <typename DesiredType,
            typename IsNotInitializerListChecker=
                typename IsNotInitializerList<DesiredType>::type,
            typename ContainerChecker=
                typename DesiredType::const_iterator>
#else
  template <typename DesiredType>
#endif
  operator DesiredType() {
    // This typedef makes sure that DesiredType is likely a
    // container and cannot be a primitive type.
    typedef typename DesiredType::const_iterator const_iterator;

    return DesiredType();
  }

  template <typename DesiredContainer>
  DesiredContainer As() {
    return this->operator DesiredContainer();
  }

 private:
  void operator=(const ContainerImpl0&);
};

// Like ContainerImpl, except that an object of NewContainerImpl type
// can be converted to an STL-style container on the *heap*.
template <typename T, size_t kCount>
class NewContainerImpl {
 public:
  // elem_ptrs is an array of kCount pointers.
  explicit NewContainerImpl(const T* elem_ptrs[]) {
    memcpy(elem_ptrs_, elem_ptrs, kCount*sizeof(*elem_ptrs));
  }

  // The compiler-generated copy constructor does exactly what we
  // want, so we don't define our own.  The copy constructor is needed
  // as the NewContainer() functions need to return a NewContainerImpl
  // object by value.

  // Converts the collection to the desired container type on the
  // heap; the caller assumes the ownership.
  //
  // We only create the target container when this function is called,
  // so there won't be any leak even if this is never called.  There
  // is no problem if this is called multiple times either (note that
  // a user cannot do that in the current design), as each caller will
  // get its own copy of the container.
  template <typename DesiredContainer>
  operator DesiredContainer*() {
    return new DesiredContainer(DerefIterator<const T>(elem_ptrs_),
                                DerefIterator<const T>(elem_ptrs_ + kCount));
  }

 private:
  const T* elem_ptrs_[kCount];

  // We don't need an assignment operator.
  void operator=(const NewContainerImpl&);
};

class NewContainerImpl0 {
 public:
  template <typename DesiredContainer>
  operator DesiredContainer*() {
    // This typedef makes sure that DesiredContainer is likely a
    // container and cannot be a primitive type.
    typedef typename DesiredContainer::const_iterator const_iterator;

    return new DesiredContainer;
  }

 private:
  void operator=(const NewContainerImpl0&);
};

}  // namespace internal


inline internal::ContainerImpl0 Container() {
  return internal::ContainerImpl0();
}

inline internal::NewContainerImpl0 NewContainer() {
  return internal::NewContainerImpl0();
}

}  // namespace gtl
}  // namespace util

// Defines Container() and NewContainer().
#include "gutil/container_literal_generated.h"

#endif  // UTIL_GTL_CONTAINER_LITERAL_H_
