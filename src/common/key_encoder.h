// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_COMMON_KEYENCODER_H
#define KUDU_COMMON_KEYENCODER_H


#include <arpa/inet.h>
#include <emmintrin.h>
#include <smmintrin.h>
#include <string.h>
#include <climits>
#include <bits/endian.h>

#include "common/types.h"
#include "gutil/macros.h"
#include "util/faststring.h"
#include "util/memory/arena.h"

// The SSE-based encoding is not yet working. Don't define this!
#undef KEY_ENCODER_USE_SSE

namespace kudu {


template<DataType Type>
struct KeyEncoderTraits {
};

template<>
struct KeyEncoderTraits<UINT8> {

  static const DataType key_type = UINT8;

  static void Encode(const void* key, faststring* dst) {
    Encode(*reinterpret_cast<const uint8_t *>(key), dst);
  }

  inline static void Encode(uint8_t key, faststring* dst) {
    dst->append(&key, sizeof(key));
  }

  static void EncodeWithSeparators(const void* key, bool is_last, faststring* dst) {
    Encode(*reinterpret_cast<const uint8_t *>(key), dst);
  }

  static void Decode(const Slice& encoded_key,
                     Arena* arena,
                     uint8_t* cell_ptr) {
    *cell_ptr = encoded_key.data()[0];
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
struct KeyEncoderTraits<INT8> {

  static const DataType key_type = INT8;

  static void Encode(const void* key, faststring* dst) {
    static const uint8_t one = 1;
    uint8_t res = *reinterpret_cast<const int8_t *>(key)
        ^ one << (sizeof(res) * CHAR_BIT - 1);
    KeyEncoderTraits<UINT8>::Encode(res, dst);
  }

  static void EncodeWithSeparators(const void* key, bool is_last, faststring* dst) {
    Encode(key, dst);
  }

  static void Decode(const Slice& encoded_key,
                     Arena* arena,
                     uint8_t* cell_ptr) {
    static const uint8_t one = 1;
    KeyEncoderTraits<UINT8>::Decode(encoded_key,
                                    arena,
                                    cell_ptr);
    *cell_ptr ^= one << (sizeof(*cell_ptr) * CHAR_BIT - 1);
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
struct KeyEncoderTraits<UINT16> {

  static const DataType key_type = UINT16;

  static void Encode(const void* key, faststring* dst) {
    Encode(*reinterpret_cast<const uint16_t *>(key), dst);
  }

  inline static void Encode(uint16_t key, faststring* dst) {
    key = htobe16(key);
    dst->append(&key, sizeof(key));
  }

  static void EncodeWithSeparators(const void* key, bool is_last, faststring* dst) {
    Encode(*reinterpret_cast<const uint16_t *>(key), dst);
  }

  static void Decode(const Slice& encoded_key,
                     Arena* arena,
                     uint8_t* cell_ptr) {
    uint16_t* key_ptr = reinterpret_cast<uint16_t*>(cell_ptr);
    const uint16_t* enc_key_ptr = reinterpret_cast<const uint16_t*>(encoded_key.data());
    *key_ptr = be16toh(*enc_key_ptr);
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
struct KeyEncoderTraits<INT16> {

  static const DataType key_type = INT16;

  static void Encode(const void* key, faststring* dst) {
    static const int16_t one = 1;
    int16_t res = *reinterpret_cast<const int16_t *>(key);
    res ^= one << (sizeof(res) * CHAR_BIT - 1);
    KeyEncoderTraits<UINT16>::Encode(res, dst);
  }

  static void EncodeWithSeparators(const void* key, bool is_last, faststring* dst) {
    Encode(key, dst);
  }

  static void Decode(const Slice& encoded_key,
                        Arena* arena,
                        uint8_t* cell_ptr) {
    static const uint16_t one = 1;
    KeyEncoderTraits<UINT16>::Decode(encoded_key,
                                     arena,
                                     cell_ptr);
    int16_t enc = *reinterpret_cast<const int16_t *>(cell_ptr);
    enc ^= one << (sizeof(enc) * CHAR_BIT - 1);
    *reinterpret_cast<int16_t *>(cell_ptr) = enc;
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
struct KeyEncoderTraits<UINT32> {

  static const DataType key_type = UINT32;

  static void Encode(const void* key, faststring* dst) {
    Encode(*reinterpret_cast<const uint32_t *>(key), dst);
  }

  inline static void Encode(uint32_t key, faststring* dst) {
    key = htobe32(key);
    dst->append(&key, sizeof(key));
  }

  static void EncodeWithSeparators(const void* key, bool is_last, faststring* dst) {
    Encode(*reinterpret_cast<const uint32_t *>(key), dst);
  }

  static void Decode(const Slice& encoded_key,
                     Arena* arena,
                     uint8_t* cell_ptr) {
    uint32_t* key_ptr = reinterpret_cast<uint32_t*>(cell_ptr);
    const uint32_t* enc_key_ptr = reinterpret_cast<const uint32_t*>(encoded_key.data());
    *key_ptr = be32toh(*enc_key_ptr);
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
struct KeyEncoderTraits<INT32> {

  static const DataType key_type = INT32;

  static void Encode(const void* key, faststring* dst) {
    static const int32_t one = 1;
    int32_t res = *reinterpret_cast<const int32_t *>(key);
    res ^= one << (sizeof(res) * CHAR_BIT - 1);
    KeyEncoderTraits<UINT32>::Encode(res, dst);
  }

  static void EncodeWithSeparators(const void* key, bool is_last, faststring* dst) {
    Encode(key, dst);
  }

  static void Decode(const Slice& encoded_key,
                     Arena* arena,
                     uint8_t* cell_ptr) {
    static const uint32_t one = 1;
    KeyEncoderTraits<UINT32>::Decode(encoded_key,
                                     arena,
                                     cell_ptr);
    int32_t enc = *reinterpret_cast<const int32_t *>(cell_ptr);
    enc ^= one << (sizeof(enc) * CHAR_BIT - 1);
    *reinterpret_cast<int32_t *>(cell_ptr) = enc;
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
struct KeyEncoderTraits<UINT64> {

  static const DataType key_type = INT64;

  static void Encode(const void* key, faststring* dst) {
    Encode(*reinterpret_cast<const uint64_t *>(key), dst);
  }

  inline static void Encode(uint64_t key, faststring* dst) {
    key = htobe64(key);
    dst->append(&key, sizeof(key));
  }

  static void EncodeWithSeparators(const void* key, bool is_last, faststring* dst) {
    Encode(*reinterpret_cast<const uint64_t *>(key), dst);
  }

  static void Decode(const Slice& encoded_key,
                     Arena* arena,
                     uint8_t* cell_ptr) {
    uint64_t* key_ptr = reinterpret_cast<uint64_t*>(cell_ptr);
    const uint64_t* enc_key_ptr = reinterpret_cast<const uint64_t*>(encoded_key.data());
    *key_ptr = be64toh(*enc_key_ptr);
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
struct KeyEncoderTraits<INT64> {

  static const DataType key_type = INT64;

  static void Encode(const void* key, faststring* dst) {
    static const int64_t one = 1;
    int64_t res = *reinterpret_cast<const int64_t *>(key);
    res ^= one << (sizeof(res) * CHAR_BIT - 1);
    KeyEncoderTraits<UINT64>::Encode(res, dst);
  }

  static void EncodeWithSeparators(const void* key, bool is_last, faststring* dst) {
    Encode(key, dst);
  }

  static void Decode(const Slice& encoded_key,
                     Arena* arena,
                     uint8_t* cell_ptr) {
    static const int64_t one = 1;
    KeyEncoderTraits<UINT64>::Decode(encoded_key,
                                     arena,
                                     cell_ptr);
    int64_t enc = *reinterpret_cast<const int64_t *>(cell_ptr);
    enc ^= one << (sizeof(enc) * CHAR_BIT - 1);
    *reinterpret_cast<int64_t *>(cell_ptr) = enc;
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
  // key starting at the beggining of the composite key column and
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
