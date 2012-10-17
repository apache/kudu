// Copyright 2009 Google Inc. All Rights Reserved.
// Based on code by austern@google.com (Matt Austern)

#ifndef UTIL_GTL_POINTER_VECTOR_H_
#define UTIL_GTL_POINTER_VECTOR_H_

#include <stddef.h>
#include <stdint.h>
#include <iterator>
using std::back_insert_iterator;
using std::iterator_traits;
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "gutil/logging-inl.h"
#include "gutil/macros.h"
#include "gutil/scoped_ptr.h"

namespace util {
namespace gtl {

// Container class PointerVector<T> is modeled after the interface
// of vector<scoped_ptr<T> >, with the important distinction that
// it compiles.  It has the same performance characteristics:
// O(1) access to the nth element, O(N) insertion in the middle, and
// amortized O(1) insertion at the end. See clause 23 of the C++ standard.
// And base/scoped_ptr.h, of course.
//
// Exceptions are:
//
// 1) You can not construct a PointerVector of multiple elements unless they're
//    all NULL, nor can you insert() multiple elements unless the pointer being
//    inserted is NULL.
// 1a) This means the iteration constructor and copy consructor are not
//     supported.
//
// 2) assignment is not supported.
//
// 3) The iterator form of insert is not supported.
//
// 4) resize() only ever fills with NULL.
//
// 5) You can't use relational operators to compare 2 PointerVectors
//
template <typename T>
class PointerVector {
 public:
  typedef scoped_ptr<T> value_type;
  typedef value_type &reference;
  typedef const value_type &const_reference;
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef value_type *pointer;
  typedef const value_type *const_pointer;

 private:
  typedef vector<T *> BaseVector;

  template <typename ValueType, typename BaseIterator>
  struct IteratorBase {
    typedef std::random_access_iterator_tag iterator_category;
    typedef typename PointerVector<T>::difference_type difference_type;

    // This doesn't work: typedef ValueType value_type;
    // because PointerVector<T>::const_iterator::value_type should not be const.
    typedef typename PointerVector<T>::value_type value_type;
    typedef ValueType& reference;
    typedef ValueType* pointer;

    friend class PointerVector<T>;

    IteratorBase() { }
    IteratorBase(const IteratorBase& other)
        : base_iterator_(other.base_iterator_) { }

    // This ctor is for "up-casting" from iterator to const_iterator.
    template <typename Other>
    IteratorBase(const IteratorBase<scoped_ptr<T>, Other>& other)
        : base_iterator_(other.base_iterator_) { }

    reference operator*() const {
      return *(operator->());
    }
    pointer operator->() const {
      return reinterpret_cast<pointer>(base_iterator_.operator->());
    }
    IteratorBase operator-(difference_type diff) const {
      return IteratorBase(base_iterator_ - diff);
    }
    IteratorBase operator+(difference_type diff) const {
      return IteratorBase(base_iterator_ + diff);
    }
    difference_type operator-(const IteratorBase& other) const {
      return base_iterator_ - other.base_iterator_;
    }
    IteratorBase &operator++() {
      ++base_iterator_;
      return *this;
    }
    IteratorBase operator++(int) {  // NOLINT (unused parameter)
      return IteratorBase(base_iterator_++);
    }
    IteratorBase &operator--() {
      --base_iterator_;
      return *this;
    }
    IteratorBase operator--(int) {  // NOLINT (unused parameter)
      return IteratorBase(base_iterator_--);
    }
    reference operator[](difference_type diff) const {
      return *IteratorBase(base_iterator_ + diff);
    }
    IteratorBase& operator+=(difference_type diff) {
      base_iterator_ += diff;
      return *this;
    }
    IteratorBase& operator-=(difference_type diff) {
      base_iterator_ -= diff;
      return *this;
    }
    friend bool operator==(const IteratorBase& a, const IteratorBase& b) {
      return a.base_iterator_ == b.base_iterator_;
    }
    friend bool operator<(const IteratorBase& a, const IteratorBase& b) {
      return a.base_iterator_ < b.base_iterator_;
    }
    friend bool operator<=(const IteratorBase& a, const IteratorBase& b) {
      return !(b < a);
    }
    friend bool operator>(const IteratorBase& a, const IteratorBase& b) {
      return b < a;
    }
    friend bool operator>=(const IteratorBase& a, const IteratorBase& b) {
      return !(a < b);
    }
    friend bool operator!=(const IteratorBase& a, const IteratorBase& b) {
      return !(a == b);
    }

   private:
    IteratorBase(const BaseIterator& base_iterator)  // NOLINT (explicit)
        : base_iterator_(base_iterator) { }
    BaseIterator base_iterator_;
  };

 public:
  typedef IteratorBase<value_type, typename BaseVector::iterator> iterator;
  typedef IteratorBase<const value_type,
                       typename BaseVector::const_iterator> const_iterator;

  typedef std::reverse_iterator<iterator> reverse_iterator;
  typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

  PointerVector() : data_() { }
  explicit PointerVector(size_t n)
      : data_(n) {
  }

  ~PointerVector();

  iterator begin() {
    return iterator(data_.begin());
  }
  iterator end() {
    return iterator(data_.end());
  }
  const_iterator begin() const {
    return const_iterator(data_.begin());
  }
  const_iterator end() const {
    return const_iterator(data_.end());
  }

  reverse_iterator rbegin() {
    return reverse_iterator(end());
  }
  const_reverse_iterator rbegin() const {
    return const_reverse_iterator(end());
  }
  reverse_iterator rend() {
    return reverse_iterator(begin());
  }
  const_reverse_iterator rend() const {
    return const_reverse_iterator(begin());
  }

  size_t size() const { return data_.size(); }
  size_t max_size() const { return data_.max_size(); }
  void resize(size_t n) {
    for (size_t i = data_.size(); i > n; --i) {
      (*this)[i - 1].reset(NULL);
    }
    data_.resize(n);
  }
  size_t capacity() const { return data_.capacity(); }
  bool empty() const { return data_.empty(); }
  void reserve(size_t n) { data_.reserve(n); }

  reference operator[](size_t n) {
    return begin().operator[](n);
  }
  const_reference operator[](size_t n) const {
    return begin().operator[](n);
  }
  reference at(size_t n) {
    return *reinterpret_cast<pointer>(&data_.at(n));
  }
  const_reference at(size_t n) const {
    return *reinterpret_cast<const_pointer>(&data_.at(n));
  }
  reference front() {
    return (*this)[0];
  }
  const_reference front() const {
    return (*this)[0];
  }
  reference back() {
    return (*this)[size() - 1];
  }
  const_reference back() const {
    return (*this)[size() - 1];
  }

  void push_back(T *x) {  // Deprecated. Use emplace_back instead.
    data_.push_back(x);
  }
  void pop_back() {
    back().reset();
    data_.pop_back();
  }
  // In C++0x, you can use vector<unique_ptr<T>> instead of PointerVector<T>,
  // however if you do so, push_back won't work.  emplace_back does, however.
  // Therefore, for ease of future adaptation, use emplace_back.
  void emplace_back(T *x) {
    data_.push_back(x);
  }

  iterator insert(iterator pos, T *x) {  // Deprecated. Use emplace instead.
    return emplace(pos, x);
  }
  void insert(iterator pos, size_t n, T *x) {
    if (x != NULL) DCHECK_EQ(1, n);
    size_t offset = pos - begin();
    data_.insert(data_.begin() + offset, n, 0);
  }
  iterator emplace(iterator pos, T *x) {
    size_t offset = pos - begin();
    data_.insert(data_.begin() + offset, x);
    return begin() + offset;
  }

  iterator erase(iterator pos);
  iterator erase(iterator first, iterator last);
  void clear() { erase(begin(), end()); }

  void swap(PointerVector<T> &x) {
    data_.swap(x.data_);
  }

 private:
  // Disallow copy and assign
  PointerVector(const PointerVector &x);
  void operator=(const PointerVector &x);

  BaseVector data_;
};

// Definitions of out-of-line member functions.

template <typename T>
PointerVector<T>::~PointerVector() {
  COMPILE_ASSERT(sizeof(T *) == sizeof(scoped_ptr<T>),
                 PointerVector_assumes_scoped_ptrs_contain_just_a_pointer);
  clear();
}

template<typename T>
typename PointerVector<T>::iterator PointerVector<T>::erase(iterator pos) {
  pos->reset(NULL);
  size_t offset = pos - begin();
  data_.erase(data_.begin() + offset);
  return pos;  // OK because erase is guaranteed not to reallocate the vector.
}

template<typename T>
typename PointerVector<T>::iterator PointerVector<T>::erase(iterator first,
                                                            iterator last) {
  for (iterator it = first; it != last; ++it) {
    it->reset(NULL);
  }
  size_t first_offset = first - begin();
  size_t last_offset = last - begin();
  data_.erase(data_.begin() + first_offset, data_.begin() + last_offset);
  return first;
}

// The following nonmember functions are part of the public vector interface.

template <typename T>
inline void swap(PointerVector<T> &x, PointerVector<T> &y) {
  x.swap(y);
}

}  // namespace gtl
}  // namespace util

#endif  // UTIL_GTL_POINTER_VECTOR_H_
