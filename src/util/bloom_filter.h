// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_BLOOM_FILTER_H
#define KUDU_UTIL_BLOOM_FILTER_H

#include <boost/scoped_array.hpp>

#include "gutil/hash/city.h"
#include "util/bitmap.h"
#include "util/slice.h"

namespace kudu {

// Probe calculated from a given key. This caches the calculated
// hash values which are necessary for probing into a Bloom Filter,
// so that when many bloom filters have to be consulted for a given
// key, we only need to calculate the hashes once.
//
// This is implemented based on the idea of double-hashing from the following paper:
//   "Less Hashing, Same Performance: Building a Better Bloom Filter"
//   Kirsch and Mitzenmacher, ESA 2006
//   http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
//
// Currently, the implementation uses the 64-bit City Hash.
// TODO: an SSE CRC32 hash is probably ~20% faster. Come back to this
// at some point.
class BloomKeyProbe : boost::noncopyable {
public:
  BloomKeyProbe(const Slice &key) {
    uint64_t h = util_hash::CityHash64(key.data(), key.size());

    // Use the top and bottom halves of the 64-bit hash
    // as the two independent hash functions for mixing.
    h_1_ = (uint32)h;
    h_2_ = (uint32)(h >> 32);
  }

  uint32_t initial_hash() const {
    return h_1_;
  }

  // Mix the given hash function with the second calculated hash
  // value. A sequence of independent hashes can be calculated
  // by repeatedly calling
  uint32_t MixHash(uint32_t h) const {
    return h + h_2_;
  }

private:

  uint32_t h_1_;
  uint32_t h_2_;
};


// Builder for a BloomFilter structure.
class BloomFilterBuilder : boost::noncopyable {
public:
  // Create a bloom filter which expects to be able to hold the given
  // number of keys with the approximately the given false positive rate.
  BloomFilterBuilder(size_t expected_count, double fp_rate);

  // Clear all entries.
  void Clear();

  // Add the given key to the bloom filter.
  void AddKey(const BloomKeyProbe &probe);

  // Return an estimate of the false positive rate.
  double false_positive_rate() const;

  int n_bytes() const {
    return n_bits_ / 8;
  }

  int n_bits() const {
    return n_bits_;
  }

  // Return a slice view into this Bloom Filter, suitable for
  // writing out to a file.
  const Slice slice() const {
    return Slice(reinterpret_cast<const char *>(&bitmap_[0]),
                 n_bytes());
  }

  // Return the number of hashes that are calculated for each entry
  // in the bloom filter.
  const size_t n_hashes() const { return n_hashes_; }

private:
  size_t n_bits_;
  scoped_array<uint8_t> bitmap_;

  size_t expected_count_;
  size_t n_hashes_;
};

// Wrapper around a byte array for reading it as a bloom filter.
class BloomFilter : boost::noncopyable {
public:
  BloomFilter(const Slice &data, size_t n_hashes);

  // Return true if the filter may contain the given key.
  bool MayContainKey(const BloomKeyProbe &probe) const;

private:
  size_t n_bits_;
  const uint8_t *bitmap_;

  size_t n_hashes_;
};

inline void BloomFilterBuilder::AddKey(const BloomKeyProbe &probe) {
  uint32_t h = probe.initial_hash();
  for (size_t i = 0; i < n_hashes_; i++) {
    uint32_t bitpos = h % n_bits_;
    BitmapSet(&bitmap_[0], bitpos);
    h = probe.MixHash(h);
  }
}

inline bool BloomFilter::MayContainKey(const BloomKeyProbe &probe) const {
  uint32_t h = probe.initial_hash();
  for (size_t i = 0; i < n_hashes_; i++) {
    uint32_t bitpos = h % n_bits_;
    if (!BitmapTest(&bitmap_[0], bitpos)) {
      return false;
    }
    h = probe.MixHash(h);
  }
  return true;
}

};
#endif
