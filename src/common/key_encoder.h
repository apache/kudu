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
#ifdef KEY_ENCODER_USE_SSE
    // Work-in-progress code for using SSE to do the string escaping.
    // This doesn't work correctly yet, and this hasn't been a serious hot spot.
    char buf[16];
    const uint8_t *p = s.data();
    size_t rem = s.size();

    __m128i xmm_zero = _mm_setzero_si128();

    while (rem > 0) {
      size_t chunk = (rem < 16) ? rem : 16;
      memset(buf, 0, sizeof(buf));
      memcpy(buf, p, chunk);
      rem -= chunk;

      __m128i xmm_chunk = _mm_load_si128(
        reinterpret_cast<__m128i *>(buf));
      uint16_t zero_mask = _mm_movemask_epi8(
        _mm_cmpeq_epi8(xmm_zero, xmm_chunk));

      zero_mask &= ((1 << chunk) - 1);

      if (PREDICT_TRUE(zero_mask == 0)) {
        dst_->append(buf, chunk);
      } else {
        // zero_mask now has bit 'n' set for each n where
        // buf[n] == '\0'
        // TODO: use the two halves of the bitmask in a lookup
        // table with pshufb?

        CHECK(0) << "TODO";
      }
    }
    uint8_t zeros[] = {0, 0};
    if (!is_last) {
      dst_->append(zeros, 2);
    }
#else
    for (int i = 0; i < s.size(); i++) {
      if (PREDICT_FALSE(s[i] == '\0')) {
        dst->append("\x00\x01", 2);
      } else {
        dst->push_back(s[i]);
      }
    }
    if (!is_last) {
      dst->append("\x00\x00", 2);
    }
#endif
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
