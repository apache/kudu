// Copyright (c) 2013, Cloudera, inc.

#include <math.h>

#include "util/bloom_filter.h"
#include "util/bitmap.h"

namespace kudu {

static double kNaturalLog2 = 0.69314;

static size_t ComputeBytesForFPRate(size_t expected_count, double fp_rate) {
  CHECK_GT(fp_rate, 0);
  CHECK_LT(fp_rate, 1);

  double n_bits = -(double)expected_count * log(fp_rate)
    / kNaturalLog2 / kNaturalLog2;
  int n_bytes = (int)ceil(n_bits / 8);
  CHECK_GT(n_bytes, 0)
    << "expected_count: " << expected_count
    << " fp_rate: " << fp_rate;
  return n_bytes;
}

static int ComputeOptimalHashCount(size_t n_bits, size_t elems) {
  int n_hashes = n_bits * kNaturalLog2 / elems;
  if (n_hashes < 1) n_hashes = 1;
  return n_hashes;
}

BloomFilterBuilder::BloomFilterBuilder(size_t expected_count, double fp_rate) :
  n_bits_(ComputeBytesForFPRate(expected_count, fp_rate) * 8),
  bitmap_(new uint8_t[n_bytes()]),
  expected_count_(expected_count),
  n_hashes_(ComputeOptimalHashCount(n_bits_, expected_count))
{
  Clear();
}

void BloomFilterBuilder::Clear() {
  memset(&bitmap_[0], 0, n_bits_ * 8);
}

double BloomFilterBuilder::false_positive_rate() const {
  CHECK(expected_count_ != 0)
    << "expected_count_ not initialized: can't call this function on "
    << "a BloomFilter initialized from external data";

  return pow(1 - exp(-(double)n_hashes_ * expected_count_ / n_bits_), n_hashes_);
}

BloomFilter::BloomFilter(const Slice &data, size_t n_hashes) :
  n_bits_(data.size() * 8),
  bitmap_(reinterpret_cast<const uint8_t *>(data.data())),
  n_hashes_(n_hashes)
{}



} // namespace kudu
