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
#include "kudu/cfile/type_encodings.h"

#include <array>
#include <cstddef>
#include <memory>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>

#include "kudu/cfile/binary_dict_block.h" // IWYU pragma: keep
#include "kudu/cfile/binary_plain_block.h" // IWYU pragma: keep
#include "kudu/cfile/binary_prefix_block.h" // IWYU pragma: keep
#include "kudu/cfile/block_encodings.h"
#include "kudu/cfile/block_handle.h"
#include "kudu/cfile/bshuf_block.h" // IWYU pragma: keep
#include "kudu/cfile/plain_bitmap_block.h" // IWYU pragma: keep
#include "kudu/cfile/plain_block.h" // IWYU pragma: keep
#include "kudu/cfile/rle_block.h" // IWYU pragma: keep
#include "kudu/common/types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/singleton.h"
#include "kudu/gutil/strings/substitute.h"

using std::array;
using std::make_pair;
using std::make_unique;
using std::pair;
using std::unique_ptr;
using std::unordered_map;

namespace kudu {
namespace cfile {

// Base template class to help instantiate classes with specific BlockBuilder and BlockDecoder
// classes.
template<class Builder, class Decoder>
struct EncodingTraits {
  static unique_ptr<BlockBuilder> CreateBlockBuilder(const WriterOptions* options) {
    return make_unique<Builder>(options);
  }

  static unique_ptr<BlockDecoder> CreateBlockDecoder(
                                   // https://bugs.llvm.org/show_bug.cgi?id=44598
                                   // NOLINTNEXTLINE(performance-unnecessary-value-param)
                                   scoped_refptr<BlockHandle> block,
                                   CFileIterator* /*parent_cfile_iter*/) {
    return make_unique<Decoder>(std::move(block));
  }
};

template<DataType Type, EncodingType Encoding>
struct DataTypeEncodingTraits {};

// Instantiate this template to get static access to the type traits.
template<DataType Type, EncodingType Encoding> struct TypeEncodingTraits
    : public DataTypeEncodingTraits<Type, Encoding> {

  static constexpr const EncodingType kEncodingType = Encoding;
};

// Generic, fallback, partial specialization that should work for all
// fixed size types.
template<DataType Type>
struct DataTypeEncodingTraits<Type, PLAIN_ENCODING>
    : public EncodingTraits<PlainBlockBuilder<Type>, PlainBlockDecoder<Type>> {};

// Generic, fallback, partial specialization that should work for all
// fixed size types.
template<DataType Type>
struct DataTypeEncodingTraits<Type, BIT_SHUFFLE>
    : public EncodingTraits<BShufBlockBuilder<Type>, BShufBlockDecoder<Type>> {};

// Template specialization for plain encoded string as they require a
// specific encoder/decoder.
template<>
struct DataTypeEncodingTraits<BINARY, PLAIN_ENCODING>
    : public EncodingTraits<BinaryPlainBlockBuilder, BinaryPlainBlockDecoder> {};

// Template specialization for packed bitmaps
template<>
struct DataTypeEncodingTraits<BOOL, PLAIN_ENCODING>
    : public EncodingTraits<PlainBitMapBlockBuilder, PlainBitMapBlockDecoder> {};

// Template specialization for RLE encoded bitmaps
template<>
struct DataTypeEncodingTraits<BOOL, RLE>
    : public EncodingTraits<RleBitMapBlockBuilder, RleBitMapBlockDecoder> {};

// Template specialization for plain encoded string as they require a
// specific encoder \/decoder.
template<>
struct DataTypeEncodingTraits<BINARY, PREFIX_ENCODING>
    : public EncodingTraits<BinaryPrefixBlockBuilder, BinaryPrefixBlockDecoder> {};

// Template for dictionary encoding
template<>
struct DataTypeEncodingTraits<BINARY, DICT_ENCODING>
    : public EncodingTraits<BinaryDictBlockBuilder, BinaryDictBlockDecoder> {
  static unique_ptr<BlockDecoder> CreateBlockDecoder(
      scoped_refptr<BlockHandle> block, CFileIterator* parent_cfile_iter) {
    return make_unique<BinaryDictBlockDecoder>(std::move(block), parent_cfile_iter);
  }
};

template<DataType IntType>
struct DataTypeEncodingTraits<IntType, RLE>
    : public EncodingTraits<RleIntBlockBuilder<IntType>, RleIntBlockDecoder<IntType>> {};

template<typename TypeEncodingTraitsClass>
TypeEncodingInfo::TypeEncodingInfo(TypeEncodingTraitsClass /*unused*/)
    : encoding_type_(TypeEncodingTraitsClass::kEncodingType),
      create_builder_func_(TypeEncodingTraitsClass::CreateBlockBuilder),
      create_decoder_func_(TypeEncodingTraitsClass::CreateBlockDecoder) {
}

unique_ptr<BlockDecoder> TypeEncodingInfo::CreateBlockDecoder(
    scoped_refptr<BlockHandle> block, CFileIterator* parent_cfile_iter) const {
  return create_decoder_func_(std::move(block), parent_cfile_iter);
}

unique_ptr<BlockBuilder> TypeEncodingInfo::CreateBlockBuilder(
    const WriterOptions* options) const {
  return create_builder_func_(options);
}

struct EncodingMapHash {
  size_t operator()(const pair<DataType, EncodingType>& pair) const noexcept {
    return (pair.first << 5) + pair.second;
  }
};

// A resolver for encodings, keeps all the allowed type<->encoding
// combinations. The first combination to be added to the map
// becomes the default encoding for the type.
class TypeEncodingResolver {
 public:
  Status GetTypeEncodingInfo(DataType t,
                             EncodingType e,
                             const TypeEncodingInfo** out) const {
    if (e == AUTO_ENCODING) {
      e = GetDefaultEncoding(t);
    }
    const TypeEncodingInfo* info = FindPointeeOrNull(mapping_, make_pair(t, e));
    if (PREDICT_FALSE(info == nullptr)) {
      return Status::NotSupported(
          strings::Substitute("encoding $1 not supported for type $0",
                              DataType_Name(t),
                              EncodingType_Name(e)));
    }
    *out = info;
    return Status::OK();
  }

  EncodingType GetDefaultEncoding(DataType t) const {
    DCHECK_LE(t, kDataTypeMaxIdx);
    return default_mapping_[t];
  }

  // Add the encoding mappings
  // the first encoder/decoder to be
  // added to the mapping becomes the default
 private:
  friend class Singleton<TypeEncodingResolver>;

  TypeEncodingResolver() {
    default_mapping_.fill(UNKNOWN_ENCODING);
    AddMapping<UINT8, BIT_SHUFFLE>();
    AddMapping<UINT8, PLAIN_ENCODING>();
    AddMapping<UINT8, RLE>();
    AddMapping<INT8, BIT_SHUFFLE>();
    AddMapping<INT8, PLAIN_ENCODING>();
    AddMapping<INT8, RLE>();
    AddMapping<UINT16, BIT_SHUFFLE>();
    AddMapping<UINT16, PLAIN_ENCODING>();
    AddMapping<UINT16, RLE>();
    AddMapping<INT16, BIT_SHUFFLE>();
    AddMapping<INT16, PLAIN_ENCODING>();
    AddMapping<INT16, RLE>();
    AddMapping<UINT32, BIT_SHUFFLE>();
    AddMapping<UINT32, RLE>();
    AddMapping<UINT32, PLAIN_ENCODING>();
    AddMapping<INT32, BIT_SHUFFLE>();
    AddMapping<INT32, PLAIN_ENCODING>();
    AddMapping<INT32, RLE>();
    AddMapping<UINT64, BIT_SHUFFLE>();
    AddMapping<UINT64, PLAIN_ENCODING>();
    AddMapping<UINT64, RLE>();
    AddMapping<INT64, BIT_SHUFFLE>();
    AddMapping<INT64, PLAIN_ENCODING>();
    AddMapping<INT64, RLE>();
    AddMapping<FLOAT, BIT_SHUFFLE>();
    AddMapping<FLOAT, PLAIN_ENCODING>();
    AddMapping<DOUBLE, BIT_SHUFFLE>();
    AddMapping<DOUBLE, PLAIN_ENCODING>();
    AddMapping<BINARY, DICT_ENCODING>();
    AddMapping<BINARY, PLAIN_ENCODING>();
    AddMapping<BINARY, PREFIX_ENCODING>();
    AddMapping<BOOL, RLE>();
    AddMapping<BOOL, PLAIN_ENCODING>();
    AddMapping<INT128, BIT_SHUFFLE>();
    AddMapping<INT128, PLAIN_ENCODING>();
    // TODO: Add 128 bit support to RLE
    // AddMapping<INT128, RLE>();
  }

  template<DataType type, EncodingType encoding>
  void AddMapping() {
    static_assert(type <= kDataTypeMaxIdx, "unexpected type for encoder mapping");
    if (default_mapping_[type] == UNKNOWN_ENCODING) {
      default_mapping_[type] = encoding;
    }
    const auto ins_info = mapping_.emplace(
        make_pair(type, encoding),
        new TypeEncodingInfo(TypeEncodingTraits<type, encoding>()));
    DCHECK(ins_info.second);
  }

  static constexpr const size_t kDataTypeMaxIdx = DataType::INT128;
  array<EncodingType, kDataTypeMaxIdx + 1> default_mapping_;

  unordered_map<pair<DataType, EncodingType>,
                unique_ptr<const TypeEncodingInfo>,
                EncodingMapHash> mapping_;

  DISALLOW_COPY_AND_ASSIGN(TypeEncodingResolver);
};

Status TypeEncodingInfo::Get(const TypeInfo* typeinfo,
                             EncodingType encoding,
                             const TypeEncodingInfo** out) {
  return Singleton<TypeEncodingResolver>::get()->GetTypeEncodingInfo(
      typeinfo->physical_type(), encoding, out);
}

EncodingType TypeEncodingInfo::GetDefaultEncoding(const TypeInfo* typeinfo) {
  return Singleton<TypeEncodingResolver>::get()->GetDefaultEncoding(
      typeinfo->physical_type());
}

}  // namespace cfile
}  // namespace kudu

