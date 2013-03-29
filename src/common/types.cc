// Copyright (c) 2012, Cloudera, inc.

#include <boost/shared_ptr.hpp>
#include <tr1/unordered_map>

#include "gutil/singleton.h"

#include "types.h"

namespace kudu {

using std::tr1::unordered_map;
using boost::shared_ptr;

template<typename TypeTraitsClass>
TypeInfo::TypeInfo(TypeTraitsClass t) :
  type_(TypeTraitsClass::type),
  name_(TypeTraitsClass::name()),
  size_(TypeTraitsClass::size),
  append_func_(TypeTraitsClass::AppendDebugStringForValue),
  compare_func_(TypeTraitsClass::Compare)
{
}

void TypeInfo::AppendDebugStringForValue(const void *ptr, string *str) const {
  append_func_(ptr, str);
}

int TypeInfo::Compare(const void *lhs, const void *rhs) const {
  return compare_func_(lhs, rhs);
}

class TypeInfoResolver {
public:
  const TypeInfo &GetTypeInfo(DataType t) {
    const TypeInfo *type_info = mapping_[t].get();
    CHECK(type_info != NULL) <<
      "Bad type: " << t;
    return *type_info;
  }

private:
  TypeInfoResolver() {
    AddMapping<UINT32>();
    AddMapping<STRING>();
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

const TypeInfo &GetTypeInfo(DataType type) {
  return Singleton<TypeInfoResolver>::get()->GetTypeInfo(type);
}

int DataTypeTraits<STRING>::Compare(const void *lhs, const void *rhs) {
  const Slice *lhs_slice = reinterpret_cast<const Slice *>(lhs);
  const Slice *rhs_slice = reinterpret_cast<const Slice *>(rhs);
  return lhs_slice->compare(*rhs_slice);
}

int DataTypeTraits<UINT32>::Compare(const void *lhs, const void *rhs) {
  uint32_t lhs_int = *reinterpret_cast<const uint32_t *>(lhs);
  uint32_t rhs_int = *reinterpret_cast<const uint32_t *>(rhs);
  if (lhs_int < rhs_int) {
    return -1;
  } else if (lhs_int > rhs_int) {
    return 1;
  } else {
    return 0;
  }
}

} // namespace kudu
