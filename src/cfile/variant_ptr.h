// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_UTIL_VARIANT_PTR_H
#define KUDU_UTIL_VARIANT_PTR_H

#include "cfile.pb.h"

namespace kudu  {
namespace cfile {

// TODO: move this out of the cfile package, along with the enums
// and other type info stuff

#ifndef NDEBUG
#define KUDU_TYPESAFE_POINTERS
#endif

class VariantPointer {
public:
  VariantPointer(void *ptr, DataType type_) :
    ptr_(ptr)
#ifdef KUDU_TYPESAFE_POINTERS
    ,type_(o.type_)
#endif
  {}

  VariantPointer(const VariantPointer &o) :
    ptr_(o.ptr_)
#ifdef KUDU_TYPESAFE_POINTERS
    ,type_(o.type_)
#endif
  {}

  VariantPointer &operator=(const VariantPointer &o) {
    ptr_ = o.ptr_;
#ifdef KUDU_TYPESAFE_POINTERS
    type_ = o.type_;
#endif
  }

  void *ptr() {
    return ptr_;
  }

  const void *ptr() const {
    return ptr_;
  }

  template<typename T>
  T as() {
    return reinterpret_cast<T>(ptr_);
  }

  template<typename T>
  const T as_const() const {
    return reinterpret_cast<T>(ptr_);
  }

private:
  void *ptr_;

#ifdef KUDU_TYPESAFE_POINTERS
  DataType type_;
#endif
};

} // namespace cfile
} // namespace kudu

#endif
