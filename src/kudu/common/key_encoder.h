// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_COMMON_KEYENCODER_H
#define KUDU_COMMON_KEYENCODER_H


#include <arpa/inet.h>
#include <string.h>
#include <climits>
#include <bits/endian.h>

#include "kudu/common/types.h"
#include "kudu/gutil/endian.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/strings/memutil.h"
#include "kudu/gutil/type_traits.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/status.h"

// The SSE-based encoding is not yet working. Don't define this!
#undef KEY_ENCODER_USE_SSE

namespace kudu {

template<DataType Type, class Enable = void>
struct KeyEncoderTraits {
};


// This complicated-looking template magic defines a specialization of the
// KeyEncoderTraits struct for any integral type. This avoids a bunch of
// code duplication for all of our different size/signed-ness variants.
template<DataType Type>
struct KeyEncoderTraits<Type, typename base::enable_if<
                                base::is_integral<
                                  typename DataTypeTraits<Type>::cpp_type
                                  >::value
                                  >::type
                        > {
  static const DataType key_type = Type;

 private:
  typedef typename DataTypeTraits<Type>::cpp_type cpp_type;
  typedef typename MathLimits<cpp_type>::UnsignedType unsigned_cpp_type;

  static unsigned_cpp_type SwapEndian(unsigned_cpp_type x) {
    switch (sizeof(x)) {
      case 1: return x;
      case 2: return BigEndian::FromHost16(x);
      case 4: return BigEndian::FromHost32(x);
      case 8: return BigEndian::FromHost64(x);
      default: LOG(FATAL) << "bad type: " << x;
    }
    return 0;
  }

 public:
  static void Encode(cpp_type key, faststring* dst) {
    Encode(&key, dst);
  }

  static void Encode(const void* key_ptr, faststring* dst) {
    unsigned_cpp_type key_unsigned;
    memcpy(&key_unsigned, key_ptr, sizeof(key_unsigned));

    // To encode signed integers, swap the MSB.
    if (MathLimits<cpp_type>::kIsSigned) {
      key_unsigned ^= 1UL << (sizeof(key_unsigned) * CHAR_BIT - 1);
    }
    key_unsigned = SwapEndian(key_unsigned);
    dst->append(&key_unsigned, sizeof(key_unsigned));
  }

  static void EncodeWithSeparators(const void* key, bool is_last, faststring* dst) {
    Encode(key, dst);
  }

  static Status DecodeKeyPortion(Slice* encoded_key,
                                 bool is_last,
                                 Arena* arena,
                                 uint8_t* cell_ptr) {
    if (PREDICT_FALSE(encoded_key->size() < sizeof(cpp_type))) {
      return Status::InvalidArgument("key too short", encoded_key->ToDebugString());
    }

    unsigned_cpp_type val;
    memcpy(&val,  encoded_key->data(), sizeof(cpp_type));
    val = SwapEndian(val);
    if (MathLimits<cpp_type>::kIsSigned) {
      val ^= 1UL << (sizeof(val) * CHAR_BIT - 1);
    }
    memcpy(cell_ptr, &val, sizeof(val));
    encoded_key->remove_prefix(sizeof(cpp_type));
    return Status::OK();
  }
};

template<>
struct KeyEncoderTraits<STRING> {

  static const DataType key_type = STRING;

  static void Encode(const void* key, faststring* dst) {
    Encode(*reinterpret_cast<const Slice*>(key), dst);
  }

  // simple slice encoding that just adds to the buffer
  inline static void Encode(const Slice& s, faststring* dst) {
    dst->append(s.data(),s.size());
  }
  static void EncodeWithSeparators(const void* key, bool is_last, faststring* dst) {
    EncodeWithSeparators(*reinterpret_cast<const Slice*>(key), is_last, dst);
  }

  // slice encoding that uses a separator to retain lexicographic
  // comparability
  inline static void EncodeWithSeparators(const Slice& s, bool is_last, faststring* dst) {
    if (is_last) {
      dst->append(s.data(), s.size());
    } else {
      // If we're a middle component of a composite key, we need to add a \x00
      // at the end in order to separate this component from the next one. However,
      // if we just did that, we'd have issues where a key that actually has
      // \x00 in it would compare wrong, so we have to instead add \x00\x00, and
      // encode \x00 as \x00\x01.
      for (int i = 0; i < s.size(); i++) {
        if (PREDICT_FALSE(s[i] == '\0')) {
          dst->append("\x00\x01", 2);
        } else {
          dst->push_back(s[i]);
        }
      }
      dst->append("\x00\x00", 2);

      // TODO: this implementation isn't as fast as it could be. There was an
      // aborted attempt at an SSE-based implementation here at one point, but
      // it didn't work so got canned. Worth looking into this to improve
      // composite key performance in the future.
    }
  }

  static Status DecodeKeyPortion(Slice* encoded_key,
                               bool is_last,
                               Arena* arena,
                               uint8_t* cell_ptr) {
    if (is_last) {
      Slice* dst_slice = reinterpret_cast<Slice *>(cell_ptr);
      if (PREDICT_FALSE(!arena->RelocateSlice(*encoded_key, dst_slice))) {
        return Status::RuntimeError("OOM");
      }
      encoded_key->remove_prefix(encoded_key->size());
      return Status::OK();
    }

    uint8_t* separator = static_cast<uint8_t*>(memmem(encoded_key->data(), encoded_key->size(),
                                                      "\0\0", 2));
    if (PREDICT_FALSE(separator == NULL)) {
      return Status::InvalidArgument("Missing separator after composite key string component",
                                     encoded_key->ToDebugString());
    }

    uint8_t* src = encoded_key->mutable_data();
    int max_len = separator - src;
    uint8_t* dst_start = static_cast<uint8_t*>(arena->AllocateBytes(max_len));
    uint8_t* dst = dst_start;

    for (int i = 0; i < max_len; i++) {
      if (i >= 1 && src[i - 1] == '\0' && src[i] == '\1') {
        continue;
      }
      *dst++ = src[i];
    }

    int real_len = dst - dst_start;
    Slice slice(dst_start, real_len);
    memcpy(cell_ptr, &slice, sizeof(Slice));
    encoded_key->remove_prefix(max_len + 2);
    return Status::OK();
  }
};

// Currently unsupported
template<>
struct KeyEncoderTraits<BOOL> {

  static const DataType key_type = BOOL;

  static void Encode(const void* key, faststring* dst) {
    LOG(FATAL) << "BOOL keys are presently unsupported";
  }

  static void EncodeWithSeparators(const void* key, bool is_last, faststring* dst) {
    Encode(key, dst);
  }

  static Status DecodeKeyPortion(Slice* encoded_key,
                               bool is_last,
                               Arena* arena,
                               uint8_t* cell_ptr) {
    LOG(FATAL) << "BOOL keys are presently unsupported";
  }
};

// The runtime version of the key encoder
class KeyEncoder {
 public:

  // Encodes the provided key to the provided faststring
  void Encode(const void* key, faststring* dst) const {
    encode_func_(key, dst);
  }

  // Special encoding for composite keys.
  void Encode(const void* key, bool is_last, faststring* dst) const {
    encode_with_separators_func_(key, is_last, dst);
  }

  void ResetAndEncode(const void* key, faststring* dst) const {
    dst->clear();
    Encode(key, dst);
  }


  // Decode the next component out of the composite key pointed to by '*encoded_key'
  // into *cell_ptr.
  // After decoding encoded_key is advanced forward such that it contains the remainder
  // of the composite key.
  // 'is_last' should be true when we expect that this component is the last (or only) component
  // of the composite key.
  // Any indirect data (eg strings) are allocated out of 'arena'.
  Status Decode(Slice* encoded_key,
                bool is_last,
                Arena* arena,
                uint8_t* cell_ptr) const {
    return decode_key_portion_func_(encoded_key, is_last, arena, cell_ptr);
  }

 private:
  friend class EncoderResolver;
  template<typename EncoderTraitsClass>
  explicit KeyEncoder(EncoderTraitsClass t)
    : encode_func_(EncoderTraitsClass::Encode),
      encode_with_separators_func_(EncoderTraitsClass::EncodeWithSeparators),
      decode_key_portion_func_(EncoderTraitsClass::DecodeKeyPortion),
      key_type_(EncoderTraitsClass::key_type) {
  }

  typedef void (*EncodeFunc)(const void* key, faststring* dst);
  const EncodeFunc encode_func_;
  typedef void (*EncodeWithSeparatorsFunc)(const void* key, bool is_last,
                                           faststring* dst);
  const EncodeWithSeparatorsFunc encode_with_separators_func_;

  typedef Status (*DecodeKeyPortionFunc)(Slice* enc_key, bool is_last,
                                       Arena* arena, uint8_t* cell_ptr);
  const DecodeKeyPortionFunc decode_key_portion_func_;

  const DataType key_type_;

 private:
  DISALLOW_COPY_AND_ASSIGN(KeyEncoder);
};

extern const KeyEncoder &GetKeyEncoder(DataType type);

} // namespace kudu

#endif
