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
#include "kudu/gutil/type_traits.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memory/arena.h"

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

  static void Decode(const Slice& encoded_key,
                     Arena* arena,
                     uint8_t* cell_ptr) {
    unsigned_cpp_type val;
    memcpy(&val,  &encoded_key[0], sizeof(cpp_type));
    val = SwapEndian(val);
    if (MathLimits<cpp_type>::kIsSigned) {
      val ^= 1UL << (sizeof(val) * CHAR_BIT - 1);
    }
    memcpy(cell_ptr, &val, sizeof(val));
  }

  static void DecodeKeyPortion(const Slice& encoded_key_portion,
                               bool is_last,
                               Arena* arena,
                               uint8_t* cell_ptr,
                               size_t* offset) {
    Decode(encoded_key_portion, arena, cell_ptr);
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

  static void Decode(const Slice& encoded_key,
                     Arena* arena,
                     uint8_t* cell_ptr) {
    Slice* dst_slice = reinterpret_cast<Slice *>(cell_ptr);
    CHECK(arena->RelocateSlice(encoded_key, dst_slice))
        << "could not allocate space in the arena";
  }

  static void DecodeKeyPortion(const Slice& encoded_key_portion,
                               bool is_last,
                               Arena* arena,
                               uint8_t* cell_ptr,
                               size_t* offset) {
    if (is_last) {
      Decode(encoded_key_portion, arena, cell_ptr);
    } else {
      const uint8_t *start = encoded_key_portion.data();
      const uint8_t* curr = start;
      const uint8_t *prev = NULL;
      const uint8_t *end = start + encoded_key_portion.size();
      size_t real_size = 0;
      while (curr < end) {
        if (PREDICT_FALSE(*curr == 0x00)) {
          if (PREDICT_TRUE(prev != NULL) && *prev == 0x00) {
            // If we found the separator (\x00\x00), terminate the
            // loop.
            real_size = curr - start - 1;
            break;
          }
        }
        prev = curr++;
      }
      if (curr == end) {
        LOG(FATAL) << "unable to decode malformed key (" << encoded_key_portion << "), it is in the"
            " middle of a composite key, but is not followed by a separator";
      }
      Slice slice(start, real_size);
      Decode(slice, arena, cell_ptr);
      *offset = real_size + 2; // 2 refers to the length of the separator.
    }
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

  static void Decode(const Slice& encoded_key,
                     Arena* arena,
                     uint8_t* cell_ptr) {
    LOG(FATAL) << "BOOL keys are presently unsupported";
  }

  static void DecodeKeyPortion(const Slice& encoded_key_portion,
                               bool is_last,
                               Arena* arena,
                               uint8_t* cell_ptr,
                               size_t* offset) {
    Decode(encoded_key_portion, arena, cell_ptr);
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

  // Decodes the specified encoded key into memory pointed by
  // 'cell_ptr' (which is usually returned by
  // ContiguousRow::mutable_cell_ptr()).  For string data types,
  // 'arena' must be initialized, as it's used for allocating indirect
  // strings.
  void Decode(const Slice& encoded_key,
              Arena* arena,
              uint8_t* cell_ptr) const {
    decode_func_(encoded_key, arena, cell_ptr);
  }

  // Similar to Decode above, but meant to be used for variable
  // length datatypes (currently only STRING) inside composite
  // keys. 'encoded_key_portion' refers to the slice of a composite
  // key starting at the beginning of the composite key column and
  // 'offset' refers to the offset relative to the start of
  // 'encoded_key_portion'.
  //
  // For example: if the encoded composite key is (123, "abcdef",
  // "ghi", "jkl") and we want to decode the "ghi" column,
  // 'encoded_key_portion' would start at "g"; the returned value
  // would be a Slice containing "jkl" and 'offset' would be the
  // byte offset of "j" within 'encoded_key_portion'.
  //
  // See: Schema::DecodeRowKey() for example usage.
  //
  // NOTE: currently this method may only be used with the STRING
  // datatype.
  void Decode(const Slice& encoded_key_portion,
              bool is_last,
              Arena* arena,
              uint8_t* cell_ptr,
              size_t* offset) const {
    decode_key_portion_func_(encoded_key_portion, is_last, arena, cell_ptr, offset);
  }

 private:
  friend class EncoderResolver;
  template<typename EncoderTraitsClass>
  explicit KeyEncoder(EncoderTraitsClass t)
    : encode_func_(EncoderTraitsClass::Encode),
      encode_with_separators_func_(EncoderTraitsClass::EncodeWithSeparators),
      decode_func_(EncoderTraitsClass::Decode),
      decode_key_portion_func_(EncoderTraitsClass::DecodeKeyPortion),
      key_type_(EncoderTraitsClass::key_type) {
  }

  typedef void (*EncodeFunc)(const void* key, faststring* dst);
  const EncodeFunc encode_func_;
  typedef void (*EncodeWithSeparatorsFunc)(const void* key, bool is_last,
                                           faststring* dst);
  const EncodeWithSeparatorsFunc encode_with_separators_func_;

  typedef void (*DecodeFunc)(const Slice& enc_key, Arena* arena, uint8_t* cell_ptr);
  const DecodeFunc decode_func_;

  typedef void (*DecodeKeyPortionFunc)(const Slice& enc_key, bool is_last,
                                       Arena* arena, uint8_t* cell_ptr, size_t* offset);
  const DecodeKeyPortionFunc decode_key_portion_func_;

  const DataType key_type_;

 private:
  DISALLOW_COPY_AND_ASSIGN(KeyEncoder);
};

extern const KeyEncoder &GetKeyEncoder(DataType type);

} // namespace kudu

#endif
