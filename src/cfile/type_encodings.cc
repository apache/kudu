// Copyright (c) 2013, Cloudera, inc.
#include "cfile/type_encodings.h"

#include <boost/noncopyable.hpp>
#include <utility>
#include <tr1/unordered_map>
#include <glog/logging.h>

namespace kudu {
namespace cfile {

using std::tr1::unordered_map;
using boost::shared_ptr;

template<typename TypeEncodingTraitsClass>
TypeEncodingInfo::TypeEncodingInfo(TypeEncodingTraitsClass t)
    : type_(TypeEncodingTraitsClass::type),
      encoding_type_(TypeEncodingTraitsClass::encoding_type),
      create_builder_func_(TypeEncodingTraitsClass::CreateBlockBuilder),
      create_decoder_func_(TypeEncodingTraitsClass::CreateBlockDecoder) {
}

Status TypeEncodingInfo::CreateBlockDecoder(BlockDecoder **bd,
                                            const Slice &slice) const {
  return create_decoder_func_(bd, slice);
}

Status TypeEncodingInfo::CreateBlockBuilder(
    BlockBuilder **bb, const WriterOptions *options) const {
  return create_builder_func_(bb, options);
}

struct EncodingMapHash {
  size_t operator()(pair<DataType, EncodingType> pair) const {
    return (pair.first + 31) ^ pair.second;
  }
};

// A resolver for encodings, keeps all the allowed type<->encoding
// combinations. The first combination to be added to the map
// becomes the default encoding for the type.
class TypeEncodingResolver {
 public:
  const TypeEncodingInfo &GetTypeEncodingInfo(DataType t, EncodingType e) {
    const TypeEncodingInfo *type_info = mapping_[make_pair(t, e)].get();
    CHECK(type_info != NULL) << "Unsupported type/encoding pair: " << t << ", "
                             << e;
    return *type_info;
  }

  const EncodingType GetDefaultEncoding(DataType t) {
    return default_mapping_[t];
  }

  // Add the encoding mappings
  // the first encoder/decoder to be
  // added to the mapping becomes the default
 private:
  TypeEncodingResolver() {
    AddMapping<UINT8, PLAIN>();
    AddMapping<INT8, PLAIN>();
    AddMapping<UINT16, PLAIN>();
    AddMapping<INT16, PLAIN>();
    AddMapping<UINT32, GROUP_VARINT>();
    AddMapping<UINT32, PLAIN>();
    AddMapping<INT32, PLAIN>();
    AddMapping<UINT64, PLAIN>();
    AddMapping<INT64, PLAIN>();
    AddMapping<STRING, PREFIX>();
    AddMapping<STRING, PLAIN>();
    AddMapping<BOOL, PLAIN>();
  }

  template<DataType type, EncodingType encoding> void AddMapping() {
    TypeEncodingTraits<type, encoding> traits;
    pair<DataType, EncodingType> encoding_for_type = make_pair(type, encoding);
    if (mapping_.find(encoding_for_type) == mapping_.end()) {
      default_mapping_.insert(make_pair(type, encoding));
    }
    mapping_.insert(
        make_pair(make_pair(type, encoding),
                  shared_ptr<TypeEncodingInfo>(new TypeEncodingInfo(traits))));
  }

  unordered_map<pair<DataType, EncodingType>,
      shared_ptr<const TypeEncodingInfo>,
      EncodingMapHash > mapping_;

  unordered_map<DataType, EncodingType, std::tr1::hash<size_t> > default_mapping_;

  friend class Singleton<TypeEncodingResolver>;
  DISALLOW_COPY_AND_ASSIGN(TypeEncodingResolver);
};

const TypeEncodingInfo &TypeEncodingInfo::Get(DataType type,
                                              EncodingType encoding) {
  return Singleton<TypeEncodingResolver>::get()->GetTypeEncodingInfo(type,
                                                                     encoding);
}

const EncodingType TypeEncodingInfo::GetDefaultEncoding(DataType type) {
  return Singleton<TypeEncodingResolver>::get()->GetDefaultEncoding(type);
}

}  // namespace cfile
}  // namespace kudu

