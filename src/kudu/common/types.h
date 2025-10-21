// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>

#include <glog/logging.h>

#include "kudu/common/array_cell_view.h"
#include "kudu/common/common.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/util/int128.h"
#include "kudu/util/int128_util.h" // IWYU pragma: keep
#include "kudu/util/slice.h"
// IWYU pragma: no_include "kudu/util/status.h"

namespace kudu {

// The size of the in-memory format of the largest
// type we support.
const int kLargestTypeSize = sizeof(Slice);

class TypeInfo;

// This is the important bit of this header:
// given a type enum, get the TypeInfo about it.
extern const TypeInfo* GetTypeInfo(DataType type);

// Similar to GetTypeInfo() above, but for one-dimensional arrays  with elements
// of the specified scalar type.
extern const TypeInfo* GetArrayTypeInfo(DataType element_type);

// ArrayTypeDescriptor provides information on array data types. This descriptor
// is capable of representing arrays of primitive (e.g., 1D arrays) and nested
// types with the arbitrary level of nesting by a means it's encapsulated into
// the NestedTypeDescriptor and TypeInfo constructs below.
class ArrayTypeDescriptor {
 public:
  explicit ArrayTypeDescriptor(const TypeInfo* elem_type_info)
      : elem_type_info_(elem_type_info) {
    DCHECK(elem_type_info_);
  }
  const TypeInfo* elem_type_info() const {
    return elem_type_info_;
  }

 private:
  const TypeInfo* elem_type_info_;
};

// NestedTypeDescriptor adds information on non-scalar data types into TypeInfo.
class NestedTypeDescriptor {
 public:
  // Enumeration of nested (non-scalar) data types that NestedTypeDescriptor
  // is used to reprsent.
  enum Type {
    ARRAY,
    //MAP,
    //STRUCT,
  };

  // NestedTypeDescriptor for an array.
  explicit NestedTypeDescriptor(ArrayTypeDescriptor desc)
      : type_(Type::ARRAY),
        descriptor_(desc)  {
    descriptor_.array = desc;
  }

  bool is_array() const {
    return type_ == Type::ARRAY;
  }

  const ArrayTypeDescriptor& array() const {
    DCHECK_EQ(Type::ARRAY, type_);
    return descriptor_.array;
  }

 private:
  // The nested type (array, map, struct, etc.) that the descriptor represents.
  const Type type_;

  // A union to store the information on a descriptor of given type type_'.
  union Descriptor {
    explicit Descriptor(ArrayTypeDescriptor desc)
        : array(desc) {
    }
    ~Descriptor() = default;

    ArrayTypeDescriptor array;
  } descriptor_;
};

// Information about a given type.
// This is a runtime equivalent of the DataTypeTraits template below.
class TypeInfo {
 public:
  static bool is_array(const TypeInfo& tinfo) {
    if (tinfo.type_ != NESTED) {
      DCHECK(!tinfo.nested_type_info_.has_value());
      return false;
    }
    const auto& desc = tinfo.nested_type_info_;
    DCHECK(desc.has_value());
    return desc.has_value() && desc->is_array();
  }

  // Returns the type mentioned in the schema.
  DataType type() const { return type_; }
  // Returns the type used to actually store the data.
  DataType physical_type() const { return physical_type_; }
  const std::string& name() const { return name_; }
  size_t size() const { return size_; }
  const NestedTypeDescriptor* nested_type_info() const {
    return nested_type_info_.has_value() ? &nested_type_info_.value() : nullptr;
  }
  void AppendDebugStringForValue(const void* ptr, std::string* str) const;
  int Compare(const void* lhs, const void* rhs) const;
  // Returns true if increment(a) is equal to b.
  bool AreConsecutive(const void* a, const void* b) const;
  void CopyMinValue(void* dst) const {
    memcpy(dst, min_value_, size_);
  }
  bool IsMinValue(const void* value) const {
    return Compare(value, min_value_) == 0;
  }
  bool IsMaxValue(const void* value) const {
    return max_value_ != nullptr && Compare(value, max_value_) == 0;
  }
  bool is_virtual() const {
    return is_virtual_;
  }
  bool is_array() const {
    return is_array(*this);
  }

 private:
  friend class TypeInfoResolver;

  template<typename TypeTraitsClass>
  explicit TypeInfo(TypeTraitsClass unused,
                    std::optional<NestedTypeDescriptor> nt_info = std::nullopt);

  const DataType type_;
  const DataType physical_type_;
  const std::string name_;
  const size_t size_;
  const void* const min_value_;
  // The maximum value of the type, or null if the type has no max value.
  const void* const max_value_;
  // Whether or not the type may only be used in projections, not tablet schemas.
  const bool is_virtual_;
  // Nested type information: present if any only if type_ == NESTED.
  std::optional<NestedTypeDescriptor> nested_type_info_;

  typedef void (*AppendDebugFunc)(const void*, std::string*);
  const AppendDebugFunc append_func_;

  typedef int (*CompareFunc)(const void*, const void*);
  const CompareFunc compare_func_;

  typedef bool (*AreConsecutiveFunc)(const void*, const void*);
  const AreConsecutiveFunc are_consecutive_func_;
};

// Given TypeInfo of an array, return pointer to the TypeInfo for the array's
// elements or nullptr if the specified TypeInfo isn't of a NESTED array type.
const TypeInfo* GetArrayElementTypeInfo(const TypeInfo& typeinfo);

template<DataType Type>
struct DataTypeTraits {};

template<DataType Type>
struct ArrayDataTypeTraits {};

template<DataType Type>
static int GenericCompare(const void* lhs, const void* rhs) {
  typedef typename DataTypeTraits<Type>::cpp_type CppType;
  CppType lhs_int = UnalignedLoad<CppType>(lhs);
  CppType rhs_int = UnalignedLoad<CppType>(rhs);
  if (lhs_int < rhs_int) {
    return -1;
  }
  if (lhs_int > rhs_int) {
    return 1;
  }
  return 0;
}

template<DataType Type>
static int AreIntegersConsecutive(const void* a, const void* b) {
  typedef typename DataTypeTraits<Type>::cpp_type CppType;
  CppType a_int = UnalignedLoad<CppType>(a);
  CppType b_int = UnalignedLoad<CppType>(b);
  // Avoid overflow by checking relative position first.
  return a_int < b_int && a_int + 1 == b_int;
}

template<DataType Type>
static int AreFloatsConsecutive(const void* a, const void* b) {
  typedef typename DataTypeTraits<Type>::cpp_type CppType;
  CppType a_float = UnalignedLoad<CppType>(a);
  CppType b_float = UnalignedLoad<CppType>(b);
  return a_float < b_float && std::nextafter(a_float, b_float) == b_float;
}

template<>
struct DataTypeTraits<UINT8> {
  typedef uint8_t cpp_type;
  static constexpr const DataType physical_type = UINT8;
  static constexpr const char* name() {
    return "uint8";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(SimpleItoa(*reinterpret_cast<const uint8_t*>(val)));
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<UINT8>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreIntegersConsecutive<UINT8>(a, b);
  }
  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
  static const cpp_type* max_value() {
    return &MathLimits<cpp_type>::kMax;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<INT8> {
  typedef int8_t cpp_type;
  static constexpr const DataType physical_type = INT8;
  static constexpr const char* name() {
    return "int8";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(SimpleItoa(*reinterpret_cast<const int8_t*>(val)));
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<INT8>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreIntegersConsecutive<INT8>(a, b);
  }
  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
  static const cpp_type* max_value() {
    return &MathLimits<cpp_type>::kMax;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<UINT16> {
  typedef uint16_t cpp_type;
  static constexpr const DataType physical_type = UINT16;
  static constexpr const char* name() {
    return "uint16";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(SimpleItoa(*reinterpret_cast<const uint16_t*>(val)));
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<UINT16>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreIntegersConsecutive<UINT16>(a, b);
  }
  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
  static const cpp_type* max_value() {
    return &MathLimits<cpp_type>::kMax;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<INT16> {
  typedef int16_t cpp_type;
  static constexpr const DataType physical_type = INT16;
  static constexpr const char* name() {
    return "int16";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(SimpleItoa(*reinterpret_cast<const int16_t*>(val)));
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<INT16>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreIntegersConsecutive<INT16>(a, b);
  }
  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
  static const cpp_type* max_value() {
    return &MathLimits<cpp_type>::kMax;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<UINT32> {
  typedef uint32_t cpp_type;
  static constexpr const DataType physical_type = UINT32;
  static constexpr const char* name() {
    return "uint32";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(SimpleItoa(*reinterpret_cast<const uint32_t*>(val)));
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<UINT32>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreIntegersConsecutive<UINT32>(a, b);
  }
  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
  static const cpp_type* max_value() {
    return &MathLimits<cpp_type>::kMax;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<INT32> {
  typedef int32_t cpp_type;
  static constexpr const DataType physical_type = INT32;
  static constexpr const char* name() {
    return "int32";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(SimpleItoa(*reinterpret_cast<const int32_t*>(val)));
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<INT32>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreIntegersConsecutive<INT32>(a, b);
  }
  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
  static const cpp_type* max_value() {
    return &MathLimits<cpp_type>::kMax;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<UINT64> {
  typedef uint64_t cpp_type;
  static constexpr const DataType physical_type = UINT64;
  static constexpr const char* name() {
    return "uint64";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(SimpleItoa(*reinterpret_cast<const uint64_t*>(val)));
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<UINT64>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreIntegersConsecutive<UINT64>(a, b);
  }
  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
  static const cpp_type* max_value() {
    return &MathLimits<cpp_type>::kMax;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<INT64> {
  typedef int64_t cpp_type;
  static constexpr const DataType physical_type = INT64;
  static constexpr const char* name() {
    return "int64";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(SimpleItoa(*reinterpret_cast<const int64_t*>(val)));
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<INT64>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreIntegersConsecutive<INT64>(a, b);
  }
  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kMin;
  }
  static const cpp_type* max_value() {
    return &MathLimits<cpp_type>::kMax;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<INT128> {
  typedef int128_t cpp_type;
  static constexpr const DataType physical_type = INT128;
  static constexpr const char* name() {
    return "int128";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(SimpleItoa(UnalignedLoad<int128_t>(val)));
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<INT128>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreIntegersConsecutive<INT128>(a, b);
  }
  static const cpp_type* min_value() {
    return &INT128_MIN;
  }
  static const cpp_type* max_value() {
    return &INT128_MAX;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<FLOAT> {
  typedef float cpp_type;
  static constexpr const DataType physical_type = FLOAT;
  static constexpr const char* name() {
    return "float";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(SimpleFtoa(*reinterpret_cast<const float*>(val)));
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<FLOAT>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreFloatsConsecutive<FLOAT>(a, b);
  }
  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kNegInf;
  }
  static const cpp_type* max_value() {
    return &MathLimits<cpp_type>::kPosInf;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<DOUBLE> {
  typedef double cpp_type;
  static constexpr const DataType physical_type = DOUBLE;
  static constexpr const char* name() {
    return "double";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(SimpleDtoa(*reinterpret_cast<const double*>(val)));
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<DOUBLE>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreFloatsConsecutive<DOUBLE>(a, b);
  }
  static const cpp_type* min_value() {
    return &MathLimits<cpp_type>::kNegInf;
  }
  static const cpp_type* max_value() {
    return &MathLimits<cpp_type>::kPosInf;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<BINARY> {
  typedef Slice cpp_type;
  static constexpr const DataType physical_type = BINARY;
  static constexpr const char* name() {
    return "binary";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    const Slice* s = reinterpret_cast<const Slice*>(val);
    str->push_back('"');
    str->append(strings::CHexEscape(s->ToString()));
    str->push_back('"');
  }
  static int Compare(const void* lhs, const void* rhs) {
    const Slice* lhs_slice = reinterpret_cast<const Slice*>(lhs);
    const Slice* rhs_slice = reinterpret_cast<const Slice*>(rhs);
    return lhs_slice->compare(*rhs_slice);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    const Slice* a_slice = reinterpret_cast<const Slice*>(a);
    const Slice* b_slice = reinterpret_cast<const Slice*>(b);
    size_t a_size = a_slice->size();
    size_t b_size = b_slice->size();

    // Strings are consecutive if the larger is equal to the lesser with an
    // additional null byte.

    return a_size + 1 == b_size &&
        (*b_slice)[a_size] == 0 &&
        *a_slice == Slice(b_slice->data(), a_size);
  }
  static const cpp_type* min_value() {
    static Slice s("");
    return &s;
  }
  static const cpp_type* max_value() {
    return nullptr;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

template<>
struct DataTypeTraits<BOOL> {
  typedef bool cpp_type;
  static constexpr const DataType physical_type = BOOL;
  static constexpr const char* name() {
    return "bool";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    str->append(*reinterpret_cast<const bool*>(val) ? "true" : "false");
  }
  static int Compare(const void* lhs, const void* rhs) {
    return GenericCompare<BOOL>(lhs, rhs);
  }
  static bool AreConsecutive(const void* a, const void* b) {
    return AreIntegersConsecutive<BOOL>(a, b);
  }
  static const cpp_type* min_value() {
    static bool b = false;
    return &b;
  }
  static const cpp_type* max_value() {
    static bool b = true;
    return &b;
  }
  static constexpr bool IsVirtual() {
    return false;
  }
};

// Base class for types that are derived, that is that have some other type as the
// physical representation.
template<DataType PhysicalType>
struct DerivedTypeTraits {
  typedef typename DataTypeTraits<PhysicalType>::cpp_type cpp_type;
  static constexpr const DataType physical_type = PhysicalType;

  static void AppendDebugStringForValue(const void* val, std::string* str) {
    DataTypeTraits<PhysicalType>::AppendDebugStringForValue(val, str);
  }

  static int Compare(const void* lhs, const void* rhs) {
    return DataTypeTraits<PhysicalType>::Compare(lhs, rhs);
  }

  static bool AreConsecutive(const void* a, const void* b) {
    return DataTypeTraits<PhysicalType>::AreConsecutive(a, b);
  }

  static const cpp_type* min_value() {
    return DataTypeTraits<PhysicalType>::min_value();
  }

  static const cpp_type* max_value() {
    return DataTypeTraits<PhysicalType>::max_value();
  }
  static constexpr bool IsVirtual() {
    return DataTypeTraits<PhysicalType>::IsVirtual();
  }
};

template<>
struct DataTypeTraits<STRING> : public DerivedTypeTraits<BINARY>{
  static constexpr const char* name() {
    return "string";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    const Slice* s = reinterpret_cast<const Slice*>(val);
    str->push_back('"');
    str->append(strings::Utf8SafeCEscape(s->ToString()));
    str->push_back('"');
  }
};


template<>
struct DataTypeTraits<UNIXTIME_MICROS> : public DerivedTypeTraits<INT64>{
  constexpr static const int kMicrosInSecond = 1000L * 1000L;
  constexpr static const char* kDateFormat = "%Y-%m-%dT%H:%M:%S";
  constexpr static const char* kDateMicrosAndTzFormat = "%s.%06dZ";

  static constexpr const char* name() {
    return "unixtime_micros";
  }

  static void AppendDebugStringForValue(const void* val, std::string* str) {
    int64_t timestamp_micros = *reinterpret_cast<const int64_t*>(val);
    time_t secs_since_epoch = timestamp_micros / kMicrosInSecond;
    // If the time is negative we need to take into account that any microseconds
    // will actually decrease the time in seconds by one.
    int remaining_micros = static_cast<int>(timestamp_micros % kMicrosInSecond);
    if (remaining_micros < 0) {
      secs_since_epoch--;
      remaining_micros = kMicrosInSecond - std::abs(remaining_micros);
    }
    struct tm tm_info;
    gmtime_r(&secs_since_epoch, &tm_info);
    char time_up_to_secs[24];
    strftime(time_up_to_secs, sizeof(time_up_to_secs), kDateFormat, &tm_info);
    char time[34];
    snprintf(time, sizeof(time), kDateMicrosAndTzFormat, time_up_to_secs, remaining_micros);
    str->append(time);
  }
};

template<>
struct DataTypeTraits<DATE> : public DerivedTypeTraits<INT32>{
  typedef int32_t cpp_type;
  static constexpr int32_t kMinValue = -719162; // mktime(0001-01-01)
  static constexpr int32_t kMaxValue = 2932896; // mktime(9999-12-31)

  static constexpr const char* name() {
    return "date";
  }

  static void AppendDebugStringForValue(const void* val, std::string* str);

  static const cpp_type* min_value() {
    static int32_t value = kMinValue;
    return &value;
  }
  static const cpp_type* max_value() {
    static int32_t value = kMaxValue;
    return &value;
  }
  static bool IsValidValue(int32_t val) {
    return val >= kMinValue && val <= kMaxValue;
  }
};

template<>
struct DataTypeTraits<DECIMAL32> : public DerivedTypeTraits<INT32>{
  static constexpr const char* name() {
    return "decimal";
  }
  // AppendDebugStringForValue appends the (string representation of) the
  // underlying integer value with the "_D32" suffix as there's no "full"
  // type information available to format it.
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    DataTypeTraits<physical_type>::AppendDebugStringForValue(val, str);
    str->append("_D32");
  }
};

template<>
struct DataTypeTraits<DECIMAL64> : public DerivedTypeTraits<INT64>{
  static constexpr const char* name() {
    return "decimal";
  }
  // AppendDebugStringForValue appends the (string representation of) the
  // underlying integer value with the "_D64" suffix as there's no "full"
  // type information available to format it.
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    DataTypeTraits<physical_type>::AppendDebugStringForValue(val, str);
    str->append("_D64");
  }
};

template<>
struct DataTypeTraits<DECIMAL128> : public DerivedTypeTraits<INT128>{
  static constexpr const char* name() {
    return "decimal";
  }
  // AppendDebugStringForValue appends the (string representation of) the
  // underlying integer value with the "_D128" suffix as there's no "full"
  // type information available to format it.
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    DataTypeTraits<physical_type>::AppendDebugStringForValue(val, str);
    str->append("_D128");
  }
};

template<>
struct DataTypeTraits<IS_DELETED> : public DerivedTypeTraits<BOOL>{
  static constexpr const char* name() {
    return "is_deleted";
  }
  static constexpr bool IsVirtual() {
    return true;
  }
};

template<>
struct DataTypeTraits<VARCHAR> : public DerivedTypeTraits<BINARY>{
  static constexpr const char* name() {
    return "varchar";
  }
  static void AppendDebugStringForValue(const void* val, std::string* str) {
    const Slice* s = reinterpret_cast<const Slice*>(val);
    str->push_back('"');
    str->append(strings::Utf8SafeCEscape(s->ToString()));
    str->push_back('"');
  }
};

template<>
struct ArrayDataTypeTraits<UINT8> {
  static const char* name() {
    return "uint8 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<INT8> {
  static const char* name() {
    return "int8 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<UINT16> {
  static const char* name() {
    return "uint16 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<INT16> {
  static const char* name() {
    return "int16 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<UINT32> {
  static const char* name() {
    return "uint32 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<INT32> {
  static const char* name() {
    return "int32 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<UINT64> {
  static const char* name() {
    return "uint64 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<INT64> {
  static const char* name() {
    return "int64 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<INT128> {
  static const char* name() {
    return "int128 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<FLOAT> {
  static const char* name() {
    return "float 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<DOUBLE> {
  static const char* name() {
    return "double 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<BOOL> {
  static const char* name() {
    return "bool 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<BINARY> {
  static const char* name() {
    return "binary 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<STRING> {
  static const char* name() {
    return "string 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<UNIXTIME_MICROS> {
  static const char* name() {
    return "unixtime_micros 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<DATE> {
  static const char* name() {
    return "date 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<DECIMAL32> {
  static const char* name() {
    return "decimal (32 bit) 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<DECIMAL64> {
  static const char* name() {
    return "decimal (64 bit) 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<DECIMAL128> {
  static const char* name() {
    return "decimal (128 bit) 1d-array";
  }
};

template<>
struct ArrayDataTypeTraits<VARCHAR> {
  static const char* name() {
    return "varchar 1d-array";
  }
};

// Instantiate this template to get static access to the type traits.
template<DataType datatype>
struct TypeTraits : public DataTypeTraits<datatype> {
  typedef typename DataTypeTraits<datatype>::cpp_type cpp_type;

  static constexpr const DataType type = datatype;
  static constexpr const size_t size = sizeof(cpp_type);
};

template<DataType ARRAY_ELEMENT_TYPE>
struct ArrayTypeTraits : public ArrayDataTypeTraits<ARRAY_ELEMENT_TYPE> {
  typedef Slice cpp_type;
  typedef typename TypeTraits<ARRAY_ELEMENT_TYPE>::cpp_type element_cpp_type;

  static const DataType type = DataType::NESTED;
  static const DataType physical_type = DataType::BINARY;
  static const DataType element_type = ARRAY_ELEMENT_TYPE;

  static const size_t size = sizeof(Slice);

  static void AppendDebugStringForValue(const void* val, std::string* str) {
    static constexpr const char* const kErrCorruption = "corrupted array cell data";
    const Slice* cell_ptr = reinterpret_cast<const Slice*>(val);
    if (!cell_ptr->data() || cell_ptr->empty()) {
      // This is a NULL array cell.
      str->append("NULL");
      return;
    }
    ArrayCellMetadataView view(cell_ptr->data(), cell_ptr->size());
    const auto s = view.Init();
    DCHECK(s.ok());
    if (PREDICT_FALSE(!s.ok())) {
      str->append(kErrCorruption);
      return;
    }
    const size_t elem_num = view.elem_num();
    if (elem_num == 0) {
      str->append("[]");
      return;
    }
    DCHECK_NE(0, elem_num);
    const uint8_t* data_ptr = view.data_as(ARRAY_ELEMENT_TYPE);
    if (PREDICT_FALSE(!data_ptr)) {
      str->append(kErrCorruption);
      return;
    }
    str->append("[");
    const auto* validity = view.not_null_bitmap();
    for (size_t idx = 0; idx < elem_num; ++idx) {
      if (idx != 0) {
        str->append(", ");
      }
      // Null validity bitmap means all elements are valid.
      if (validity && !BitmapTest(validity, idx)) {
        str->append("NULL");
      } else {
        DataTypeTraits<ARRAY_ELEMENT_TYPE>::AppendDebugStringForValue(data_ptr, str);
      }
      data_ptr += sizeof(typename TypeTraits<ARRAY_ELEMENT_TYPE>::cpp_type);
    }
    str->append("]");
  }

  // Compare two arrays. If any of the array cells is null, the arrays aren't
  // equal: this follows the standard notation of comparison of two NULLs in
  // SQL: 'NULL = NULL' evaluates to 'false'. However: '[NULL] = [NULL]'
  // evaluates to 'true'.
  static int Compare(const void* lhs, const void* rhs) {
    const Slice* lhs_cell_ptr = reinterpret_cast<const Slice*>(lhs);
    if (!lhs_cell_ptr->data() || lhs_cell_ptr->empty()) {
      return -1;
    }
    const Slice* rhs_cell_ptr = reinterpret_cast<const Slice*>(rhs);
    if (!rhs_cell_ptr->data() || rhs_cell_ptr->empty()) {
      return -1;
    }

    ArrayCellMetadataView lhs_view(lhs_cell_ptr->data(), lhs_cell_ptr->size());
    if (const auto s = lhs_view.Init(); PREDICT_FALSE(!s.ok())) {
      DCHECK(false) << s.ToString();
      return -1;
    }
    ArrayCellMetadataView rhs_view(rhs_cell_ptr->data(), rhs_cell_ptr->size());
    if (const auto s = rhs_view.Init(); PREDICT_FALSE(!s.ok())) {
      DCHECK(false) << s.ToString();
      return -1;
    }
    return Compare(lhs_view, rhs_view, std::numeric_limits<size_t>::max());
  }

  // Return true if increment(a) is equal to b. For arrays, let's define
  // increment as adding an extra NULL element in the end. So, two arrays
  // are consecutive if the longer one is equal to the shorter one with an
  // additional trailing NULL element. This is consistent with the array
  // comparison rules above, where [0, 1] < [0, 1, NULL],
  // but [0, 1, NULL] < [0, 2].
  static bool AreConsecutive(const void* a, const void* b) {

    // If any of the arrays is null, they cannot be consecutive.
    const Slice* a_cell_ptr = reinterpret_cast<const Slice*>(a);
    if (!a_cell_ptr->data() || a_cell_ptr->empty()) {
      return false;
    }
    const Slice* b_cell_ptr = reinterpret_cast<const Slice*>(b);
    if (!b_cell_ptr->data() || b_cell_ptr->empty()) {
      return false;
    }

    ArrayCellMetadataView a_view(a_cell_ptr->data(), a_cell_ptr->size());
    if (const auto s = a_view.Init(); PREDICT_FALSE(!s.ok())) {
      DCHECK(false) << s.ToString();
      return false;
    }
    ArrayCellMetadataView b_view(b_cell_ptr->data(), b_cell_ptr->size());
    if (const auto s = b_view.Init(); PREDICT_FALSE(!s.ok())) {
      DCHECK(false) << s.ToString();
      return false;
    }

    const size_t a_elem_num = a_view.elem_num();
    const size_t b_elem_num = b_view.elem_num();
    if (a_elem_num + 1 != b_elem_num) {
      return false;
    }

    const uint8_t* b_not_null_bitmap = b_view.not_null_bitmap();
    if (!b_not_null_bitmap || BitmapTest(b_not_null_bitmap, a_elem_num)) {
      // The trailing extra element in 'b' must be NULL if 'b' goes
      // consecutively after 'a'.
      return false;
    }
    return Compare(a_view, b_view, a_elem_num) == 0;
  }
  static const cpp_type* min_value() {
    static const cpp_type kMinVal{};
    return &kMinVal;
  }
  static const cpp_type* max_value() {
    return nullptr;
  }
  static constexpr bool IsVirtual() {
    return false;
  }

 private:
  // Compare array represented by the specified array views facades up to
  // the specified number of elements. The comparison goes element-by-element,
  // using Compare() method for the corresponding scalar type elements.
  static int Compare(const ArrayCellMetadataView& lhs,
                     const ArrayCellMetadataView& rhs,
                     size_t num_elems_to_compare) {

    if (num_elems_to_compare == 0) {
      return 0;
    }

    const size_t lhs_elems = lhs.elem_num();
    const size_t rhs_elems = rhs.elem_num();

    // The number of array elements available for comparison in each of the arrays.
    const size_t num_elems = std::min(lhs_elems, rhs_elems);
    // The number of array elements available for comparison in each of the
    // arrays, additionally capped by the 'num_elems_to_compare' parameter.
    const size_t cap_num_elems = std::min(num_elems, num_elems_to_compare);

    const uint8_t* lhs_data_ptr = lhs.data_as(ARRAY_ELEMENT_TYPE);
    DCHECK(lhs_data_ptr || lhs_elems == 0);
    const uint8_t* lhs_not_null_bitmap = lhs.not_null_bitmap();

    const uint8_t* rhs_data_ptr = rhs.data_as(ARRAY_ELEMENT_TYPE);
    DCHECK(rhs_data_ptr || rhs_elems == 0);
    const uint8_t* rhs_not_null_bitmap = rhs.not_null_bitmap();

    for (size_t idx = 0; idx < cap_num_elems; ++idx,
        lhs_data_ptr += sizeof(typename TypeTraits<ARRAY_ELEMENT_TYPE>::cpp_type),
        rhs_data_ptr += sizeof(typename TypeTraits<ARRAY_ELEMENT_TYPE>::cpp_type)) {
      if (lhs_not_null_bitmap && !BitmapTest(lhs_not_null_bitmap, idx)) {
        if (rhs_not_null_bitmap && !BitmapTest(rhs_not_null_bitmap, idx)) {
          // Both elements are NULL: continue with next elements in the arrays.
          continue;
        }
        // lhs < rhs: a non-NULL element in rhs at idx, but lhs has NULL
        // at idx, while all the pairs of elements in the arrays contain
        // same numbers (or two NULLs) up to the 'idx' position.
        return -1;
      } else {
        if (rhs_not_null_bitmap && !BitmapTest(rhs_not_null_bitmap, idx)) {
          // lhs > rhs: a non-NULL element in lhs at idx, but rhs non-NULL
          // at idx, while all the pairs of elements in the array contain
          // same numbers (or two NULLs) up to the 'idx' position.
          return 1;
        }
        // OK, it's time to compare two non-null elements at same index.
        const int res = DataTypeTraits<ARRAY_ELEMENT_TYPE>::Compare(
            lhs_data_ptr, rhs_data_ptr);
        if (res != 0) {
          return res;
        }
      }
    }
    // At this point, all pairs of elements up to 'cap_num_elems' idx contai
    // same values or two NULLs.
    if (num_elems >= num_elems_to_compare) {
      return 0;
    }
    DCHECK(lhs_elems <= num_elems_to_compare && rhs_elems <= num_elems_to_compare);
    if (lhs_elems < rhs_elems) {
      return -1;
    }
    if (lhs_elems > rhs_elems) {
      return 1;
    }

    return 0;
  }
};

class Variant final {
 public:
  Variant(DataType type, const void* value) {
    Reset(type, value);
  }

  ~Variant() {
    Clear();
  }

  template<DataType Type>
  void Reset(const typename DataTypeTraits<Type>::cpp_type& value) {
    Reset(Type, &value);
  }

  // Set the variant to the specified type/value.
  // The value must be of the relative type.
  // In case of strings, the value must be a pointer to a Slice, and the data block
  // will be copied, and released by the variant on the next set/clear call.
  //
  //  Examples:
  //      uint16_t u16 = 512;
  //      Slice slice("Hello World");
  //      variant.set(UINT16, &u16);
  //      variant.set(STRING, &slice);
  void Reset(DataType type, const void* value) {
    DCHECK(value) << "variant value must be not NULL";
    Clear();
    type_ = type;
    switch (type_) {
      case UNKNOWN_DATA:
        LOG(FATAL) << "Unreachable";
        break;
      case IS_DELETED:
      case BOOL:
        numeric_.b1 = *static_cast<const bool*>(value);
        break;
      case INT8:
        numeric_.i8 = *static_cast<const int8_t*>(value);
        break;
      case UINT8:
        numeric_.u8 = *static_cast<const uint8_t*>(value);
        break;
      case INT16:
        numeric_.i16 = *static_cast<const int16_t*>(value);
        break;
      case UINT16:
        numeric_.u16 = *static_cast<const uint16_t*>(value);
        break;
      case DATE:
      case DECIMAL32:
      case INT32:
        numeric_.i32 = *static_cast<const int32_t*>(value);
        break;
      case UINT32:
        numeric_.u32 = *static_cast<const uint32_t*>(value);
        break;
      case DECIMAL64:
      case UNIXTIME_MICROS:
      case INT64:
        numeric_.i64 = *static_cast<const int64_t*>(value);
        break;
      case UINT64:
        numeric_.u64 = *static_cast<const uint64_t*>(value);
        break;
      case DECIMAL128:
      case INT128:
        numeric_.i128 = UnalignedLoad<int128_t>(value);
        break;
      case FLOAT:
        numeric_.float_val = *static_cast<const float*>(value);
        break;
      case DOUBLE:
        numeric_.double_val = *static_cast<const double*>(value);
        break;
      case STRING:
      case VARCHAR:
      case BINARY:
      case NESTED:
        if (const Slice* str = static_cast<const Slice*>(value); !str->empty()) {
          // If str->empty(), the 'Clear()' above has already
          // set vstr_ to Slice(""). Otherwise, we need to allocate and copy the
          // user's data.
          auto* blob = new uint8_t[str->size()];
          memcpy(blob, str->data(), str->size());
          vstr_ = Slice(blob, str->size());
        }
        break;
      default:
        LOG(FATAL) << "Unknown data type: " << type_;
    }
  }

  // Set the variant to a STRING type.
  // The specified data block will be copied, and released by the variant
  // on the next set/clear call.
  void Reset(const std::string& data) {
    Slice slice(data);
    Reset(STRING, &slice);
  }

  // Set the variant to a STRING type.
  // The specified data block will be copied, and released by the variant
  // on the next set/clear call.
  void Reset(const char* data, size_t size) {
    Slice slice(data, size);
    Reset(STRING, &slice);
  }

  // Returns the type of the Variant
  DataType type() const {
    return type_;
  }

  // Returns a pointer to the internal variant value
  // The return value can be casted to the relative type()
  // The return value will be valid until the next set() is called.
  //
  //  Examples:
  //    static_cast<const int32_t*>(variant.value())
  //    static_cast<const Slice*>(variant.value())
  const void* value() const {
    switch (type_) {
      case UNKNOWN_DATA:
        LOG(FATAL) << "Attempted to access value of unknown data type";
        return nullptr;
      case IS_DELETED:
      case BOOL:
        return &numeric_.b1;
      case INT8:
        return &numeric_.i8;
      case UINT8:
        return &numeric_.u8;
      case INT16:
        return &numeric_.i16;
      case UINT16:
        return &numeric_.u16;
      case DATE:
      case DECIMAL32:
      case INT32:
        return &numeric_.i32;
      case UINT32:
        return &numeric_.u32;
      case DECIMAL64:
      case UNIXTIME_MICROS:
      case INT64:
        return &numeric_.i64;
      case UINT64:
        return &numeric_.u64;
      case DECIMAL128:
      case INT128:
        return &numeric_.i128;
      case FLOAT:
        return &numeric_.float_val;
      case DOUBLE:
        return &numeric_.double_val;
      case STRING:
      case VARCHAR:
      case BINARY:
      case NESTED:
        return &vstr_;
      default:
        LOG(FATAL) << "Unknown data type: " << type_;
        return nullptr;
    }
  }

  bool Equals(const Variant* other) const {
    if (other == nullptr || type_ != other->type_) {
      return false;
    }
    return GetTypeInfo(type_)->Compare(value(), other->value()) == 0;
  }

 private:
  void Clear() {
    // No need to delete[] zero-length vstr_, because we always ensure that
    // such a string would point to a constant "" rather than an allocated piece
    // of memory.
    if (!vstr_.empty()) {
      delete [] vstr_.mutable_data();
      vstr_.clear();
    }
  }

  union NumericValue {
    bool     b1;
    int8_t   i8;
    uint8_t  u8;
    int16_t  i16;
    uint16_t u16;
    int32_t  i32;
    uint32_t u32;
    int64_t  i64;
    uint64_t u64;
    int128_t i128;
    float    float_val;
    double   double_val;
  };

  DataType type_;
  NumericValue numeric_;
  Slice vstr_;

  DISALLOW_COPY_AND_ASSIGN(Variant);
};

}  // namespace kudu
