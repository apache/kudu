// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_COMMON_TYPES_H
#define KUDU_COMMON_TYPES_H

#include <stdint.h>
#include <string>
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

template<DataType Type>
static int GenericCompare(const void *lhs, const void *rhs) {
  typedef typename DataTypeTraits<Type>::cpp_type CppType;
  CppType lhs_int = *reinterpret_cast<const CppType *>(lhs);
  CppType rhs_int = *reinterpret_cast<const CppType *>(rhs);
  if (lhs_int < rhs_int) {
    return -1;
  } else if (lhs_int > rhs_int) {
    return 1;
  } else {
    return 0;
  }
}

template<>
struct DataTypeTraits<UINT8> {
  typedef uint8_t cpp_type;
  static const char *name() {
    return "uint8";
  }
  static void AppendDebugStringForValue(const void *val, string *str) {
    str->append(SimpleItoa(*reinterpret_cast<const uint8_t *>(val)));
  }
  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<UINT8>(lhs, rhs);
  }
};

template<>
struct DataTypeTraits<INT8> {
  typedef int8_t cpp_type;
  static const char *name() {
    return "int8";
  }
  static void AppendDebugStringForValue(const void *val, string *str) {
    str->append(SimpleItoa(*reinterpret_cast<const int8_t *>(val)));
  }
  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<INT8>(lhs, rhs);
  }
};

template<>
struct DataTypeTraits<UINT16> {
  typedef uint16_t cpp_type;
  static const char *name() {
    return "uint16";
  }
  static void AppendDebugStringForValue(const void *val, string *str) {
    str->append(SimpleItoa(*reinterpret_cast<const uint16_t *>(val)));
  }
  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<UINT16>(lhs, rhs);
  }
};

template<>
struct DataTypeTraits<INT16> {
  typedef int16_t cpp_type;
  static const char *name() {
    return "int16";
  }
  static void AppendDebugStringForValue(const void *val, string *str) {
    str->append(SimpleItoa(*reinterpret_cast<const int16_t *>(val)));
  }
  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<INT16>(lhs, rhs);
  }
};

template<>
struct DataTypeTraits<UINT32> {
  typedef uint32_t cpp_type;
  static const char *name() {
    return "uint32";
  }
  static void AppendDebugStringForValue(const void *val, string *str) {
    str->append(SimpleItoa(*reinterpret_cast<const uint32_t *>(val)));
  }
  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<UINT32>(lhs, rhs);
  }
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
  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<INT32>(lhs, rhs);
  }
};

template<>
struct DataTypeTraits<UINT64> {
  typedef uint64_t cpp_type;
  static const char *name() {
    return "uint64";
  }
  static void AppendDebugStringForValue(const void *val, string *str) {
    str->append(SimpleItoa(*reinterpret_cast<const uint64_t *>(val)));
  }
  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<UINT64>(lhs, rhs);
  }
};

template<>
struct DataTypeTraits<INT64> {
  typedef int64_t cpp_type;
  static const char *name() {
    return "int64";
  }
  static void AppendDebugStringForValue(const void *val, string *str) {
    str->append(SimpleItoa(*reinterpret_cast<const int64_t *>(val)));
  }
  static int Compare(const void *lhs, const void *rhs) {
    return GenericCompare<INT64>(lhs, rhs);
  }
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

  static int Compare(const void *lhs, const void *rhs) {
    const Slice *lhs_slice = reinterpret_cast<const Slice *>(lhs);
    const Slice *rhs_slice = reinterpret_cast<const Slice *>(rhs);
    return lhs_slice->compare(*rhs_slice);
  }

};

// Instantiate this template to get static access to the type traits.
template<DataType datatype>
struct TypeTraits : public DataTypeTraits<datatype> {
  typedef typename DataTypeTraits<datatype>::cpp_type cpp_type;

  static const DataType type = datatype;
  static const size_t size = sizeof(cpp_type);
};

}  // namespace kudu

#endif
