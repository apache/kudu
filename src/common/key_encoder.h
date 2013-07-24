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

 private:
  friend class EncoderResolver;
  template<typename EncoderTraitsClass>
  KeyEncoder(EncoderTraitsClass t)
      : encode_func_(EncoderTraitsClass::Encode),
        encode_with_separators_func_(EncoderTraitsClass::EncodeWithSeparators),
        key_type_(EncoderTraitsClass::key_type){
  }

  typedef void (*EncodeFunc)(const void* key, faststring* dst);
  const EncodeFunc encode_func_;
  typedef void (*EncodeWithSeparatorsFunc)(const void* key, bool is_last,
                                           faststring* dst);
  const EncodeWithSeparatorsFunc encode_with_separators_func_;
  const DataType key_type_;

 private:
  DISALLOW_COPY_AND_ASSIGN(KeyEncoder);
};

extern const KeyEncoder &GetKeyEncoder(DataType type);

}// namespace kudu

#endif
