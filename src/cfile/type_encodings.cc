// Copyright (c) 2013, Cloudera, inc.
#include "cfile/type_encodings.h"

#include <boost/noncopyable.hpp>
#include <utility>
#include <tr1/unordered_map>
#include <glog/logging.h>

#include "gutil/strings/substitute.h"

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
  Status GetTypeEncodingInfo(DataType t, EncodingType e,
                             const TypeEncodingInfo** out) {
    if (e == AUTO_ENCODING) {
      e = GetDefaultEncoding(t);
    }
    const TypeEncodingInfo *type_info = mapping_[make_pair(t, e)].get();
    if (PREDICT_FALSE(type_info == NULL)) {
      return Status::NotSupported(
          strings::Substitute("Unsupported type/encoding pair: $0, $1",
                              DataType_Name(t),
                              EncodingType_Name(e)));
    }
    *out = type_info;
    return Status::OK();
  }

  const EncodingType GetDefaultEncoding(DataType t) {
    return default_mapping_[t];
  }

  // Add the encoding mappings
  // the first encoder/decoder to be
  // added to the mapping becomes the default
  //
  // TODO: Fix/work around the issue with RLE/BitWriter which
  //       (currently) makes it impossible to use RLE with
  //       64-bit int types.
 private:
  TypeEncodingResolver() {
    AddMapping<UINT8, PLAIN_ENCODING>();
    AddMapping<UINT8, RLE>();
    AddMapping<INT8, PLAIN_ENCODING>();
    AddMapping<INT8, RLE>();
    AddMapping<UINT16, PLAIN_ENCODING>();
    AddMapping<UINT16, RLE>();
    AddMapping<INT16, PLAIN_ENCODING>();
    AddMapping<INT16, RLE>();
    AddMapping<UINT32, GROUP_VARINT>();
    AddMapping<UINT32, RLE>();
    AddMapping<UINT32, PLAIN_ENCODING>();
    AddMapping<INT32, PLAIN_ENCODING>();
    AddMapping<INT32, RLE>();
    AddMapping<UINT64, PLAIN_ENCODING>();
    AddMapping<INT64, PLAIN_ENCODING>();
    AddMapping<STRING, PREFIX_ENCODING>();
    AddMapping<STRING, PLAIN_ENCODING>();
    AddMapping<BOOL, RLE>();
    AddMapping<BOOL, PLAIN_ENCODING>();
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

Status TypeEncodingInfo::Get(DataType type,
                             EncodingType encoding,
                             const TypeEncodingInfo** out) {
  return Singleton<TypeEncodingResolver>::get()->GetTypeEncodingInfo(type,
                                                                     encoding,
                                                                     out);
}

const EncodingType TypeEncodingInfo::GetDefaultEncoding(DataType type) {
  return Singleton<TypeEncodingResolver>::get()->GetDefaultEncoding(type);
}

}  // namespace cfile
}  // namespace kudu

