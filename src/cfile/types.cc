// Copyright (c) 2012, Cloudera, inc.

#include <boost/scoped_ptr.hpp>
#include <tr1/unordered_map>

#include "gutil/linked_ptr.h"
#include "gutil/singleton.h"

#include "types.h"

namespace kudu {
namespace cfile {

using std::tr1::unordered_map;
using boost::scoped_ptr;

template<typename TypeTraitsClass>
TypeInfo::TypeInfo(TypeTraitsClass t) :
  type_(TypeTraitsClass::type),
  name_(TypeTraitsClass::name()),
  size_(TypeTraitsClass::size)
{
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
    mapping_.insert(make_pair(type, make_linked_ptr(new TypeInfo(traits))));
  }

  unordered_map<DataType,
                linked_ptr<const TypeInfo>,
                std::tr1::hash<size_t> > mapping_;

  friend class Singleton<TypeInfoResolver>;
  DISALLOW_COPY_AND_ASSIGN(TypeInfoResolver);
};

const TypeInfo &GetTypeInfo(DataType type) {
  return Singleton<TypeInfoResolver>::get()->GetTypeInfo(type);
}



} // namespace cfile
} // namespace kudu
