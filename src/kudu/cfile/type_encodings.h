// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CFILE_TYPE_ENCODINGS_H_
#define KUDU_CFILE_TYPE_ENCODINGS_H_

#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/plain_block.h"
#include "kudu/cfile/rle_block.h"
#include "kudu/cfile/string_plain_block.h"
#include "kudu/cfile/string_prefix_block.h"
#include "kudu/cfile/plain_bitmap_block.h"
#include "kudu/cfile/gvint_block.h"

namespace kudu { namespace cfile {

// Runtime Information for type encoding/decoding
// including the ability to build BlockDecoders and BlockBuilders
// for each supported encoding
// Mimicked after common::TypeInfo et al
class TypeEncodingInfo {
 public:

  static Status Get(DataType type, EncodingType encoding, const TypeEncodingInfo** out);

  static const EncodingType GetDefaultEncoding(DataType type);

  DataType type() const { return type_; }
  EncodingType encoding_type() const { return encoding_type_; }
  Status CreateBlockBuilder(BlockBuilder **bb, const WriterOptions *options) const;

  // Create a BlockDecoder. Sets *bd to the newly created decoder,
  // if successful, otherwise returns a non-OK Status.
  Status CreateBlockDecoder(BlockDecoder **bd, const Slice &slice) const;
 private:

  friend class TypeEncodingResolver;
  template<typename Type> TypeEncodingInfo(Type t);

  DataType type_;
  EncodingType encoding_type_;

  typedef Status (*CreateBlockBuilderFunc)(BlockBuilder **, const WriterOptions *);
  const CreateBlockBuilderFunc create_builder_func_;

  typedef Status (*CreateBlockDecoderFunc)(BlockDecoder **, const Slice &);
  const CreateBlockDecoderFunc create_decoder_func_;

  DISALLOW_COPY_AND_ASSIGN(TypeEncodingInfo);
};

template<DataType Type, EncodingType Encoding>
struct DataTypeEncodingTraits {};

// Instantiate this template to get static access to the type traits.
template<DataType Type, EncodingType Encoding> struct TypeEncodingTraits
  : public DataTypeEncodingTraits<Type, Encoding> {

  static const DataType type = Type;
  static const EncodingType encoding_type = Encoding;
};

// Generic, fallback, partial specialization that should work for all
// fixed size types.
template<DataType Type>
struct DataTypeEncodingTraits<Type, PLAIN_ENCODING> {

  static Status CreateBlockBuilder(BlockBuilder **bb, const WriterOptions *options) {
    *bb = new PlainBlockBuilder<Type>(options);
    return Status::OK();
  }

  static Status CreateBlockDecoder(BlockDecoder **bd, const Slice &slice) {
    *bd = new PlainBlockDecoder<Type>(slice);
    return Status::OK();
  }
};

// Template specialization for plain encoded string as they require a
// specific encoder/decoder.
template<>
struct DataTypeEncodingTraits<STRING, PLAIN_ENCODING> {

  static Status CreateBlockBuilder(BlockBuilder **bb, const WriterOptions *options) {
    *bb = new StringPlainBlockBuilder(options);
    return Status::OK();
  }

  static Status CreateBlockDecoder(BlockDecoder **bd, const Slice &slice) {
    *bd = new StringPlainBlockDecoder(slice);
    return Status::OK();
  }
};

// Template specialization for packed bitmaps
template<>
struct DataTypeEncodingTraits<BOOL, PLAIN_ENCODING> {

  static Status CreateBlockBuilder(BlockBuilder **bb,
                                   const WriterOptions* /* unused: options */) {
    *bb = new PlainBitMapBlockBuilder();
    return Status::OK();
  }

  static Status CreateBlockDecoder(BlockDecoder **bd, const Slice &slice) {
    *bd = new PlainBitMapBlockDecoder(slice);
    return Status::OK();
  }
};


// Template specialization for RLE encoded bitmaps
template<>
struct DataTypeEncodingTraits<BOOL, RLE> {

  static Status CreateBlockBuilder(BlockBuilder** bb,
                                   const WriterOptions* /* unused: options */) {
    *bb = new RleBitMapBlockBuilder();
    return Status::OK();
  }

  static Status CreateBlockDecoder(BlockDecoder **bd, const Slice &slice) {
    *bd = new RleBitMapBlockDecoder(slice);
    return Status::OK();
  }
};

// Template specialization for plain encoded string as they require a
// specific encoder \/decoder.
template<>
struct DataTypeEncodingTraits<STRING, PREFIX_ENCODING> {

  static Status CreateBlockBuilder(BlockBuilder **bb, const WriterOptions *options) {
    *bb = new StringPrefixBlockBuilder(options);
    return Status::OK();
  }

  static Status CreateBlockDecoder(BlockDecoder **bd, const Slice &slice) {
    *bd = new StringPrefixBlockDecoder(slice);
    return Status::OK();
  }
};

// Optimized grouping variable encoding for 32bit unsigned integers
template<>
struct DataTypeEncodingTraits<UINT32, GROUP_VARINT> {

  static Status CreateBlockBuilder(BlockBuilder **bb, const WriterOptions *options) {
    *bb = new GVIntBlockBuilder(options);
    return Status::OK();
  }

  static Status CreateBlockDecoder(BlockDecoder **bd, const Slice &slice) {
    *bd = new GVIntBlockDecoder(slice);
    return Status::OK();
  }
};

template<DataType IntType>
struct DataTypeEncodingTraits<IntType, RLE> {

  static Status CreateBlockBuilder(BlockBuilder** bb, const WriterOptions* /* unused */) {
    *bb = new RleIntBlockBuilder<IntType>();
    return Status::OK();
  }

  static Status CreateBlockDecoder(BlockDecoder** bd, const Slice& slice) {
    *bd = new RleIntBlockDecoder<IntType>(slice);
    return Status::OK();
  }
};


} // namespace cfile
} // namespace kudu

#endif /* KUDU_CFILE_TYPE_ENCODINGS_H_ */
