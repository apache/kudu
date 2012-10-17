// Copyright 2005 Google Inc. All Rights Reserved.
//
// This file defines some iterator adapters for working
// on containers where the value_type is pair<>, such as either
// hash_map<K,V>, or list<pair<>>.
//
#ifndef UTIL_GTL_ITERATOR_ADAPTORS_H_
#define UTIL_GTL_ITERATOR_ADAPTORS_H_

#include <iterator>
using std::back_insert_iterator;
using std::iterator_traits;

#include "gutil/template_util.h"
#include "gutil/type_traits.h"   // For remove_pointer

namespace util {
namespace gtl {

// util::gtl::adaptor_helper is a helper class for creating iterator adaptors.
// util::gtl::adaptor_helper uses template metaprogramming to define value_type,
// pointer, and reference.
//
// We use template metaprogramming to make a compile-time determination of
// const-ness.  To make this determination, we compare whether the reference
// type of the parameterized iterator is defined as either "value_type&" or
// "const value_type&".  When we have the const version, the templates
// detect that the reference parameter is not equal to value_type&.
template<typename It, typename Val>
struct adaptor_helper {
  typedef Val                                             value_type;
  typedef typename base::if_<
             base::type_equals_<
               typename iterator_traits<It>::reference,
               typename iterator_traits<It>::value_type&
             >::value,
             Val*,
             const Val*>::type                            pointer;
  typedef typename base::if_<
             base::type_equals_<
               typename iterator_traits<It>::reference,
               typename iterator_traits<It>::value_type&
             >::value,
             Val&,
             const Val&>::type                            reference;
  typedef typename iterator_traits<It>::difference_type   difference_type;
  typedef typename iterator_traits<It>::iterator_category iterator_category;
};

// ptr_adaptor_helper is similar to adaptor_helper, but the second argument is a
// pointer type and our resulting value_type is the pointed-to type.
//
// We don't need to worry about const-ness here because the caller only
// interacts with dereferenced values, never with references or pointers to the
// original pointer values.
template<typename It, typename PtrVal>
struct ptr_adaptor_helper {
  typedef typename base::remove_pointer<PtrVal>::type     value_type;
  typedef const PtrVal&                                   pointer;
  typedef typename base::remove_pointer<PtrVal>::type&    reference;
  typedef typename iterator_traits<It>::difference_type   difference_type;
  typedef typename iterator_traits<It>::iterator_category iterator_category;
};

}  // namespace gtl
}  // namespace util

// In both iterator adaptors, iterator_first<> and iterator_second<>,
// we build a new iterator based on a parameterized iterator type, "It".
// The value type, "_Vt" is determined by "It::value_type::first" or
// "It::value_type::second", respectively.

// iterator_first<> adapts an iterator to return the first value of a pair.
// It is equivalent to calling it->first on every value.
// Example:
//
// hash_map<string, int> values;
// values["foo"] = 1;
// values["bar"] = 2;
// for (iterator_first<hash_map<string, int>::iterator> x = values.begin();
//      x != values.end(); ++x) {
//   printf("%s", x->c_str());
// }
template<typename It,
         typename _Val = typename iterator_traits<It>::value_type::first_type>
class iterator_first {
 private:
  // Helper template to define our necessary typedefs.
  typedef typename util::gtl::adaptor_helper<It, _Val> helper;

  // The internal iterator.
  It it_;

 public:
  typedef iterator_first<It, _Val>                    iterator;
  typedef typename helper::iterator_category          iterator_category;
  typedef typename helper::value_type                 value_type;
  typedef typename helper::pointer                    pointer;
  typedef typename helper::reference                  reference;
  typedef typename helper::difference_type            difference_type;

  iterator_first() : it_() {}
  iterator_first(const It& it) : it_(it) {}  // TODO(user): explicit?

  // Allow "upcasting" from iterator_first<T*const*> to
  // iterator_first<const T*const*>.
  template<typename OtherIt>
  iterator_first(const iterator_first<OtherIt, _Val>& other)
      : it_(other.base()) {}

  // Provide access to the wrapped iterator.
  const It& base() const { return it_; }

  reference operator*() const { return it_->first; }
  pointer   operator->() const { return &(operator*()); }

  iterator& operator++() { it_++; return *this; }
  iterator  operator++(int /*unused*/) { return iterator(it_++); }
  iterator& operator--() { it_--; return *this; }
  iterator  operator--(int /*unused*/) { return iterator(it_--); }

  iterator& operator+=(const difference_type& d) { it_ += d; return *this; }
  iterator& operator-=(const difference_type& d) { it_ -= d; return *this; }
  iterator operator+(const difference_type& d) const { return it_ + d; }
  iterator operator-(const difference_type& d) const { return it_ - d; }
  friend iterator operator+(const difference_type& d, const iterator& it) {
    return it.it_ + d;
  }
  difference_type operator-(const iterator& x) { return it_ - x.it_; }
  reference operator[](const difference_type& d) { return it_[d].first; }
  value_type operator[](const difference_type& d) const { return it_[d].first; }

  friend bool operator<(const iterator& a, const iterator& b) {
    return a.it_ < b.it_;
  }
  friend bool operator>(const iterator& a, const iterator& b) {
    return a.it_ > b.it_;
  }
  friend bool operator<=(const iterator& a, const iterator& b) {
    return a.it_ <= b.it_;
  }
  friend bool operator>=(const iterator& a, const iterator& b) {
    return a.it_ >= b.it_;
  }

  bool operator==(const iterator& x) const { return it_ == x.it_; }
  bool operator!=(const iterator& x) const { return it_ != x.it_; }

  // Convenience operators to allow comparison against a
  // native iterator, for example, when using it != container.end();
  bool operator==(const It& x) const      { return it_ == x; }
  bool operator!=(const It& x) const      { return it_ != x; }
};

template<typename It>
inline iterator_first<It> make_iterator_first(const It& it) {
  return iterator_first<It>(it);
}

// iterator_second<> adapts an iterator to return the second value of a pair.
// It is equivalent to calling it->second on every value.
// Example:
//
// hash_map<string, int> values;
// values["foo"] = 1;
// values["bar"] = 2;
// for (iterator_second<hash_map<string, int>::iterator> x = values.begin();
//      x != values.end(); ++x) {
//   int v = *x;
//   printf("%d", v);
// }
template<typename It,
         typename _Val = typename iterator_traits<It>::value_type::second_type>
class iterator_second {
 private:
  // Helper template to define our necessary typedefs.
  typedef typename util::gtl::adaptor_helper<It, _Val> helper;

  // The internal iterator.
  It it_;

 public:
  typedef iterator_second<It, _Val>                   iterator;
  typedef typename helper::iterator_category          iterator_category;
  typedef typename helper::value_type                 value_type;
  typedef typename helper::pointer                    pointer;
  typedef typename helper::reference                  reference;
  typedef typename helper::difference_type            difference_type;

  iterator_second() : it_() {}
  iterator_second(const It& it) : it_(it) {}  // TODO(user): explicit?

  // Allow "upcasting" from iterator_second<T*const*> to
  // iterator_second<const T*const*>.
  template<typename OtherIt>
  iterator_second(const iterator_second<OtherIt, _Val>& other)
      : it_(other.base()) {}

  // Provide access to the wrapped iterator.
  const It& base() const { return it_; }

  reference operator*() const { return it_->second; }
  pointer   operator->() const { return &(operator*()); }

  iterator& operator++() { it_++; return *this; }
  iterator  operator++(int /*unused*/) { return iterator(it_++); }
  iterator& operator--() { it_--; return *this; }
  iterator  operator--(int /*unused*/) { return iterator(it_--); }

  iterator& operator+=(const difference_type& d) { it_ += d; return *this; }
  iterator& operator-=(const difference_type& d) { it_ -= d; return *this; }
  iterator operator+(const difference_type& d) const { return it_ + d; }
  iterator operator-(const difference_type& d) const { return it_ - d; }
  friend iterator operator+(const difference_type& d, const iterator& it) {
    return it.it_ + d;
  }
  difference_type operator-(const iterator& x) { return it_ - x.it_; }
  reference operator[](const difference_type& d) { return it_[d].second; }
  value_type operator[](const difference_type& d) const { return it_[d].second;}

  friend bool operator<(const iterator& a, const iterator& b) {
    return a.it_ < b.it_;
  }
  friend bool operator>(const iterator& a, const iterator& b) {
    return a.it_ > b.it_;
  }
  friend bool operator<=(const iterator& a, const iterator& b) {
    return a.it_ <= b.it_;
  }
  friend bool operator>=(const iterator& a, const iterator& b) {
    return a.it_ >= b.it_;
  }

  bool operator==(const iterator& x) const { return it_ == x.it_; }
  bool operator!=(const iterator& x) const { return it_ != x.it_; }

  // Convenience operators to allow comparison against a
  // native iterator, for example, when using it != container.end();
  bool operator==(const It& x) const      { return it_ == x; }
  bool operator!=(const It& x) const      { return it_ != x; }
};


// Helper function to construct an iterator.
template<typename It>
inline iterator_second<It> make_iterator_second(const It& it) {
  return iterator_second<It>(it);
}

// iterator_second_ptr<> adapts an iterator to return the dereferenced second
// value of a pair.
// It is equivalent to calling *it->second on every value.
// The same result can be achieved by composition
// iterator_ptr<iterator_second<> >
// Can be used with maps where values are regular pointers or pointers wrapped
// into linked_ptr. This iterator adaptor can be used by classes to give their
// clients access to some of their internal data without exposing too much of
// it.
//
// Example:
// class MyClass {
//  public:
//   MyClass(const string& s);
//   string DebugString() const;
// };
// typedef hash_map<string, linked_ptr<MyClass> > MyMap;
// typedef iterator_second_ptr<MyMap::iterator> MyMapValuesIterator;
// MyMap values;
// values["foo"].reset(new MyClass("foo"));
// values["bar"].reset(new MyClass("bar"));
// for (MyMapValuesIterator it = values.begin(); it != values.end(); ++it) {
//   printf("%s", it->DebugString().c_str());
// }
template<typename It,
         typename _PtrVal =
           typename iterator_traits<It>::value_type::second_type>
class iterator_second_ptr {
 private:
  // Helper template to define our necessary typedefs.
  typedef typename util::gtl::ptr_adaptor_helper<It, _PtrVal> helper;

  // The internal iterator.
  It it_;

 public:
  typedef iterator_second_ptr<It, _PtrVal>            iterator;
  typedef typename helper::iterator_category          iterator_category;
  typedef typename helper::value_type                 value_type;
  typedef typename helper::pointer                    pointer;
  typedef typename helper::reference                  reference;
  typedef typename helper::difference_type            difference_type;

  iterator_second_ptr() : it_() {}
  iterator_second_ptr(const It& it) : it_(it) {}  // TODO(user): explicit?

  // Allow "upcasting" from iterator_second_ptr<T*const*> to
  // iterator_second_ptr<const T*const*>.
  template<typename OtherIt>
  iterator_second_ptr(const iterator_second_ptr<OtherIt, _PtrVal>& other)
      : it_(other.base()) {}

  // Provide access to the wrapped iterator.
  const It& base() const { return it_; }

  reference operator*() const { return *it_->second; }
  pointer   operator->() const { return it_->second; }

  iterator& operator++() { ++it_; return *this; }
  iterator  operator++(int /*unused*/) { return iterator(it_++); }
  iterator& operator--() { --it_; return *this; }
  iterator  operator--(int /*unused*/) { return iterator(it_--); }

  iterator& operator+=(const difference_type& d) { it_ += d; return *this; }
  iterator& operator-=(const difference_type& d) { it_ -= d; return *this; }
  iterator operator+(const difference_type& d) const { return it_ + d; }
  iterator operator-(const difference_type& d) const { return it_ - d; }
  friend iterator operator+(const difference_type& d, const iterator& it) {
    return it.it_ + d;
  }
  difference_type operator-(const iterator& x) { return it_ - x.it_; }
  reference operator[](const difference_type& d) { return *(it_[d].second); }
  value_type operator[](const difference_type& d) const {
    return *(it_[d].second);
  }

  friend bool operator<(const iterator& a, const iterator& b) {
    return a.it_ < b.it_;
  }
  friend bool operator>(const iterator& a, const iterator& b) {
    return a.it_ > b.it_;
  }
  friend bool operator<=(const iterator& a, const iterator& b) {
    return a.it_ <= b.it_;
  }
  friend bool operator>=(const iterator& a, const iterator& b) {
    return a.it_ >= b.it_;
  }

  bool operator==(const iterator& x) const { return it_ == x.it_; }
  bool operator!=(const iterator& x) const { return it_ != x.it_; }

  // Convenience operators to allow comparison against a
  // native iterator, for example, when using it != container.end();
  bool operator==(const It& x) const      { return it_ == x; }
  bool operator!=(const It& x) const      { return it_ != x; }
};

// Helper function to construct an iterator.
template<typename It>
inline iterator_second_ptr<It> make_iterator_second_ptr(const It& it) {
  return iterator_second_ptr<It>(it);
}

// iterator_ptr<> adapts an iterator to return the dereferenced value.
// With this adaptor you can write *it instead of **it, or it->something instead
// of (*it)->something.
// Can be used with vectors and lists where values are regular pointers
// or pointers wrapped into linked_ptr. This iterator adaptor can be used by
// classes to give their clients access to some of their internal data without
// exposing too much of it.
//
// Example:
// class MyClass {
//  public:
//   MyClass(const string& s);
//   string DebugString() const;
// };
// typedef vector<linked_ptr<MyClass> > MyVector;
// typedef iterator_ptr<MyVector::iterator> DereferencingIterator;
// MyVector values;
// values.push_back(make_linked_ptr(new MyClass("foo")));
// values.push_back(make_linked_ptr(new MyClass("bar")));
// for (DereferencingIterator it = values.begin(); it != values.end(); ++it) {
//   printf("%s", it->DebugString().c_str());
// }
//
// Without iterator_ptr you would have to do (*it)->DebugString()

template<typename It,
         typename _PtrVal = typename iterator_traits<It>::value_type>
class iterator_ptr {
 private:
  // Helper template to define our necessary typedefs.
  typedef typename util::gtl::ptr_adaptor_helper<It, _PtrVal> helper;

  // The internal iterator.
  It it_;

 public:
  typedef iterator_ptr<It, _PtrVal>                   iterator;
  typedef typename helper::iterator_category          iterator_category;
  typedef typename helper::value_type                 value_type;
  typedef typename helper::pointer                    pointer;
  typedef typename helper::reference                  reference;
  typedef typename helper::difference_type            difference_type;

  iterator_ptr() : it_() {}
  iterator_ptr(const It& it) : it_(it) {}  // TODO(user): explicit?

  // Allow "upcasting" from iterator_ptr<T*const*> to
  // iterator_ptr<const T*const*>.
  template<typename OtherIt>
  iterator_ptr(const iterator_ptr<OtherIt, _PtrVal>& other)
      : it_(other.base()) {}

  // Provide access to the wrapped iterator.
  const It& base() const { return it_; }

  reference operator*() const { return **it_; }
  pointer   operator->() const { return *it_; }

  iterator& operator++() { ++it_; return *this; }
  iterator  operator++(int /*unused*/) { return iterator(it_++); }
  iterator& operator--() { --it_; return *this; }
  iterator  operator--(int /*unused*/) { return iterator(it_--); }

  iterator& operator+=(const difference_type& d) { it_ += d; return *this; }
  iterator& operator-=(const difference_type& d) { it_ -= d; return *this; }
  iterator operator+(const difference_type& d) const { return it_ + d; }
  iterator operator-(const difference_type& d) const { return it_ - d; }
  friend iterator operator+(const difference_type& d, const iterator& it) {
    return it.it_ + d;
  }
  difference_type operator-(const iterator& x) { return it_ - x.it_; }
  reference operator[](const difference_type& d) { return *(it_[d]); }
  value_type operator[](const difference_type& d) const { return *(it_[d]); }

  friend bool operator<(const iterator& a, const iterator& b) {
    return a.it_ < b.it_;
  }
  friend bool operator>(const iterator& a, const iterator& b) {
    return a.it_ > b.it_;
  }
  friend bool operator<=(const iterator& a, const iterator& b) {
    return a.it_ <= b.it_;
  }
  friend bool operator>=(const iterator& a, const iterator& b) {
    return a.it_ >= b.it_;
  }

  bool operator==(const iterator& x) const { return it_ == x.it_; }
  bool operator!=(const iterator& x) const { return it_ != x.it_; }

  // Convenience operators to allow comparison against a
  // native iterator, for example, when using it != container.end();
  bool operator==(const It& x) const      { return it_ == x; }
  bool operator!=(const It& x) const      { return it_ != x; }
};

// Helper function to construct an iterator.
template<typename It>
inline iterator_ptr<It> make_iterator_ptr(const It& it) {
  return iterator_ptr<It>(it);
}


template<typename map_type,
         typename iterator_type,
         typename const_iterator_type>
class iterator_view_helper {
 public:
  explicit iterator_view_helper(map_type& map) : map_(map) { }

  iterator_type begin() {
    iterator_type it = map_.begin();
    return it;
  }

  iterator_type end() {
    iterator_type it = map_.end();
    return it;
  }

  const_iterator_type begin() const {
    const_iterator_type it = static_cast<const map_type&>(map_).begin();
    return it;
  }

  const_iterator_type end() const {
    const_iterator_type it = static_cast<const map_type&>(map_).end();
    return it;
  }

  const_iterator_type cbegin() const { return this->begin(); }
  const_iterator_type cend() const { return this->end(); }

 private:
  map_type& map_;
};

// key_view and value_view provide pretty ways to iterate either the
// keys or the values of a map using range based for loops.
// Example:
//    hash_map<int, string> my_map;
//    ...
//    for (string val : value_view(my_map)) {
//      ...
//    }
template<typename map_type>
iterator_view_helper<
    map_type,
    iterator_first<typename map_type::iterator>,
    iterator_first<typename map_type::const_iterator> >
key_view(map_type& map) {
  return iterator_view_helper
      <map_type,
       iterator_first<typename map_type::iterator>,
       iterator_first<typename map_type::const_iterator> >(map);
}

template<typename map_type>
iterator_view_helper<
    const map_type&,
    iterator_first<typename map_type::const_iterator>,
    iterator_first<typename map_type::const_iterator> >
key_view(const map_type& map) {
  return iterator_view_helper
      <const map_type&,
       iterator_first<typename map_type::const_iterator>,
       iterator_first<typename map_type::const_iterator> >(map);
}

template<typename map_type>
iterator_view_helper<
    map_type,
    iterator_second<typename map_type::iterator>,
    iterator_second<typename map_type::const_iterator> >
value_view(map_type& map) {
  return iterator_view_helper
      <map_type,
       iterator_second<typename map_type::iterator>,
       iterator_second<typename map_type::const_iterator> >(map);
}

template<typename map_type>
iterator_view_helper<
    const map_type&,
    iterator_second<typename map_type::const_iterator>,
    iterator_second<typename map_type::const_iterator> >
value_view(const map_type& map) {
  return iterator_view_helper
      <const map_type&,
       iterator_second<typename map_type::const_iterator>,
       iterator_second<typename map_type::const_iterator> >(map);
}


#endif  // UTIL_GTL_ITERATOR_ADAPTORS_H_
