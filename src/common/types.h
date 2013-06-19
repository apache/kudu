// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_COMMON_TYPES_H
#define KUDU_COMMON_TYPES_H

#include <string>
#include <stdint.h>
#include "common/common.pb.h"
#include "util/slice.h"
#include "gutil/strings/numbers.h"

namespace kudu {

using std::string;
class TypeInfo;

// This is the important bit of this header:
// given a type enum, get the TypeInfo about it.
extern const TypeInfo &GetTypeInfo(DataType type);


// Information about a given type.
// This is a runtime equivalent of the TypeTraits template below.
class TypeInfo {
public:
  DataType type() const { return type_; }
  const string& name() const { return name_; }
  const size_t size() const { return size_; }
  void AppendDebugStringForValue(const void *ptr, string *str) const;
  int Compare(const void *lhs, const void *rhs) const;

private:
  friend class TypeInfoResolver;
  template<typename Type> TypeInfo(Type t);

  const DataType type_;
  const string name_;
  const size_t size_;

  typedef void (*AppendDebugFunc)(const void *, string *);
  const AppendDebugFunc append_func_;

  typedef int (*CompareFunc)(const void *, const void *);
  const CompareFunc compare_func_;
};


template<DataType Type> struct DataTypeTraits {};

template<>
struct DataTypeTraits<UINT32> {
  typedef uint32_t cpp_type;
  static const char *name() {
    return "uint32";
  }
  static void AppendDebugStringForValue(const void *val, string *str) {
    str->append(SimpleItoa(*reinterpret_cast<const uint32_t *>(val)));
  }
  static int Compare(const void *lhs, const void *rhs);
};

template<>
struct DataTypeTraits<INT32> {
  typedef int32_t cpp_type;
  static const char *name() {
    return "int32";
  }
  static void AppendDebugStringForValue(const void *val, string *str) {
    str->append(SimpleItoa(*reinterpret_cast<const int32_t *>(val)));
  }
  static int Compare(const void *lhs, const void *rhs);
};

template<>
struct DataTypeTraits<STRING> {
  typedef Slice cpp_type;
  static const char *name() {
    return "string";
  }
  static void AppendDebugStringForValue(const void *val, string *str) {
    const Slice *s = reinterpret_cast<const Slice *>(val);
    str->append(s->ToString());
  }
  static int Compare(const void *lhs, const void *rhs);
};


// Instantiate this template to get static access to the type traits.
template<DataType datatype> struct TypeTraits :
    public DataTypeTraits<datatype> {
  typedef typename DataTypeTraits<datatype>::cpp_type cpp_type;

  static const DataType type = datatype;
  static const size_t size = sizeof(cpp_type);
};

} // namespace kudu

#endif
