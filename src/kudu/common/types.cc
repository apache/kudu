// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/shared_ptr.hpp>
#include <tr1/unordered_map>

#include "kudu/gutil/singleton.h"

#include "kudu/common/types.h"

namespace kudu {

using std::tr1::unordered_map;
using boost::shared_ptr;

template<typename TypeTraitsClass>
TypeInfo::TypeInfo(TypeTraitsClass t)
  : type_(TypeTraitsClass::type),
    physical_type_(TypeTraitsClass::physical_type),
    name_(TypeTraitsClass::name()),
    size_(TypeTraitsClass::size),
    min_value_(TypeTraitsClass::min_value()),
    append_func_(TypeTraitsClass::AppendDebugStringForValue),
    compare_func_(TypeTraitsClass::Compare) {
}

void TypeInfo::AppendDebugStringForValue(const void *ptr, string *str) const {
  append_func_(ptr, str);
}

int TypeInfo::Compare(const void *lhs, const void *rhs) const {
  return compare_func_(lhs, rhs);
}

class TypeInfoResolver {
 public:
  const TypeInfo* GetTypeInfo(DataType t) {
    const TypeInfo *type_info = mapping_[t].get();
    CHECK(type_info != NULL) <<
      "Bad type: " << t;
    return type_info;
  }

 private:
  TypeInfoResolver() {
    AddMapping<UINT8>();
    AddMapping<INT8>();
    AddMapping<UINT16>();
    AddMapping<INT16>();
    AddMapping<UINT32>();
    AddMapping<INT32>();
    AddMapping<UINT64>();
    AddMapping<INT64>();
    AddMapping<TIMESTAMP>();
    AddMapping<STRING>();
    AddMapping<BOOL>();
    AddMapping<FLOAT>();
    AddMapping<DOUBLE>();
    AddMapping<BINARY>();
  }

  template<DataType type> void AddMapping() {
    TypeTraits<type> traits;
    mapping_.insert(make_pair(type, shared_ptr<TypeInfo>(new TypeInfo(traits))));
  }

  unordered_map<DataType,
                shared_ptr<const TypeInfo>,
                std::tr1::hash<size_t> > mapping_;

  friend class Singleton<TypeInfoResolver>;
  DISALLOW_COPY_AND_ASSIGN(TypeInfoResolver);
};

const TypeInfo* GetTypeInfo(DataType type) {
  return Singleton<TypeInfoResolver>::get()->GetTypeInfo(type);
}

} // namespace kudu
