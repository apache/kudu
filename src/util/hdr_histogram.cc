// Copyright (c) 2013, Cloudera,inc.
#include "util/hdr_histogram.h"

#include <algorithm>
#include <cmath>
#include <limits>

#include "gutil/atomicops.h"
#include "gutil/bits.h"
#include "util/status.h"

using base::subtle::Atomic64;
using base::subtle::NoBarrier_AtomicIncrement;
using base::subtle::NoBarrier_Store;
using base::subtle::NoBarrier_Load;
using base::subtle::NoBarrier_CompareAndSwap;

namespace kudu {

HdrHistogram::HdrHistogram()
  : initialized_(false),
    highest_trackable_value_(),
    num_significant_digits_(),
    counts_array_length_(),
    bucket_count_(),
    sub_bucket_count_(),
    sub_bucket_half_count_magnitude_(),
    sub_bucket_half_count_(),
    sub_bucket_mask_(),
    total_count_(),
    min_value_(std::numeric_limits<Atomic64>::max()),
    max_value_(),
    counts_() {
}

Status HdrHistogram::Init(uint64_t highest_trackable_value, int num_significant_digits) {
  // Verify argument validity
  if (highest_trackable_value < 2) {
    return Status::InvalidArgument("highest_trackable_value must be >= 2");
  }
  highest_trackable_value_ = highest_trackable_value;
  if ((num_significant_digits < 1) || (num_significant_digits > 5)) {
    return Status::InvalidArgument("num_significant_digits must be between 1 and 5");
  }
  num_significant_digits_ = num_significant_digits;

  uint32_t largest_value_with_single_unit_resolution =
    2 * static_cast<uint32_t>(pow(10, num_significant_digits));

  // We need to maintain power-of-two sub_bucket_count_ (for clean direct indexing) that is large enough to
  // provide unit resolution to at least largest_value_with_single_unit_resolution. So figure out
  // largest_value_with_single_unit_resolution's nearest power-of-two (rounded up), and use that:

  // The sub-buckets take care of the precision.
  // Each sub-bucket is sized to have enough bits for the requested 10^precision accuracy.
  int sub_bucket_count_magnitude = Bits::Log2Ceiling(largest_value_with_single_unit_resolution);
  sub_bucket_half_count_magnitude_ = (sub_bucket_count_magnitude >= 1) ? sub_bucket_count_magnitude - 1 : 0;

  // sub_bucket_count_ is approx. 10^num_sig_digits (as a power of 2)
  sub_bucket_count_ = pow(2, sub_bucket_half_count_magnitude_ + 1);
  sub_bucket_mask_ = sub_bucket_count_ - 1;
  sub_bucket_half_count_ = sub_bucket_count_ / 2;

  // The buckets take care of the magnitude.
  // Determine exponent range needed to support the trackable value with no overflow:
  uint64_t trackable_value = sub_bucket_count_ - 1;
  int buckets_needed = 1;
  while (trackable_value < highest_trackable_value_) {
    trackable_value <<= 1;
    buckets_needed++;
  }
  bucket_count_ = buckets_needed;

  counts_array_length_ = (bucket_count_ + 1) * sub_bucket_half_count_;
  counts_.reset(new Atomic64[counts_array_length_]());  // value-initialized

  initialized_ = true;
  return Status::OK();
}

Status HdrHistogram::CopyTo(HdrHistogram* other) const {
  if (other->initialized_) return Status::InvalidArgument("Other histogram must not be initialized");
  RETURN_NOT_OK(other->Init(highest_trackable_value_, num_significant_digits_));
  for (int i = 0; i < counts_array_length_; i++) {
    NoBarrier_Store(&other->counts_[i], NoBarrier_Load(&counts_[i]));
  }
  NoBarrier_Store(&other->total_count_, NoBarrier_Load(&total_count_));
  NoBarrier_Store(&other->min_value_, NoBarrier_Load(&min_value_));
  NoBarrier_Store(&other->max_value_, NoBarrier_Load(&max_value_));
  return Status::OK();
}

void HdrHistogram::Increment(uint64_t value) {
  IncrementBy(value, 1);
}

void HdrHistogram::IncrementBy(uint64_t value, uint64_t count) {
  // Dissect the value into bucket and sub-bucket parts, and derive index into counts array:
  int bucket_index = BucketIndex(value);
  int sub_bucket_index = SubBucketIndex(value, bucket_index);
  int counts_index = CountsArrayIndex(bucket_index, sub_bucket_index);

  // Increment bucket & total.
  NoBarrier_AtomicIncrement(&counts_[counts_index], count);
  NoBarrier_AtomicIncrement(&total_count_, count);

  // Update min, if needed.
  {
    Atomic64 min_val;
    while (PREDICT_FALSE(value < (min_val = MinValue()))) {
      Atomic64 old_val = NoBarrier_CompareAndSwap(&min_value_, min_val, value);
      if (PREDICT_TRUE(old_val == min_val)) break; // CAS success.
    }
  }

  // Update max, if needed.
  {
    Atomic64 max_val;
    while (PREDICT_FALSE(value > (max_val = MaxValue()))) {
      Atomic64 old_val = NoBarrier_CompareAndSwap(&max_value_, max_val, value);
      if (PREDICT_TRUE(old_val == max_val)) break; // CAS success.
    }
  }
}

////////////////////////////////////

int HdrHistogram::BucketIndex(uint64_t value) const {
  DCHECK(initialized_) << "Must call Init() before using object";
  if (PREDICT_FALSE(value > highest_trackable_value_)) {
    value = highest_trackable_value_;
  }
  // Here we are calculating the power-of-2 magnitude of the value with a
  // correction for precision in the first bucket.
  int pow2ceiling = Bits::Log2Ceiling64(value | sub_bucket_mask_); // Smallest power of 2 containing value.
  return pow2ceiling - (sub_bucket_half_count_magnitude_ + 1);
}

int HdrHistogram::SubBucketIndex(uint64_t value, int bucket_index) const {
  DCHECK(initialized_) << "Must call Init() before using object";
  if (PREDICT_FALSE(value > highest_trackable_value_)) {
    value = highest_trackable_value_;
  }
  // We hack off the magnitude and are left with only the relevant precision
  // portion, which gives us a direct index into the sub-bucket. TODO: Right??
  return static_cast<int>(value >> bucket_index);
}

int HdrHistogram::CountsArrayIndex(int bucket_index, int sub_bucket_index) const {
  DCHECK(initialized_) << "Must call Init() before using object";
  DCHECK(sub_bucket_index < sub_bucket_count_);
  DCHECK(bucket_index < bucket_count_);
  DCHECK(bucket_index == 0 || (sub_bucket_index >= sub_bucket_half_count_));
  // Calculate the index for the first entry in the bucket:
  // (The following is the equivalent of ((bucket_index + 1) * sub_bucket_half_count_) ):
  int bucket_base_index = (bucket_index + 1) << sub_bucket_half_count_magnitude_;
  // Calculate the offset in the bucket:
  int offset_in_bucket = sub_bucket_index - sub_bucket_half_count_;
  return bucket_base_index + offset_in_bucket;
}

uint64_t HdrHistogram::CountAt(int bucket_index, int sub_bucket_index) const {
  return counts_[CountsArrayIndex(bucket_index, sub_bucket_index)];
}

uint64_t HdrHistogram::CountInBucketForValue(uint64_t value) const {
  int bucket_index = BucketIndex(value);
  int sub_bucket_index = SubBucketIndex(value, bucket_index);
  return CountAt(bucket_index, sub_bucket_index);
}

uint64_t HdrHistogram::ValueFromIndex(int bucket_index, int sub_bucket_index) {
  return static_cast<uint64_t>(sub_bucket_index) << bucket_index;
}

////////////////////////////////////

uint64_t HdrHistogram::SizeOfEquivalentValueRange(uint64_t value) const {
  int bucket_index = BucketIndex(value);
  int sub_bucket_index = SubBucketIndex(value, bucket_index);
  uint64_t distance_to_next_value =
    (1 << ((sub_bucket_index >= sub_bucket_count_) ? (bucket_index + 1) : bucket_index));
  return distance_to_next_value;
}

uint64_t HdrHistogram::LowestEquivalentValue(uint64_t value) const {
  int bucket_index = BucketIndex(value);
  int sub_bucket_index = SubBucketIndex(value, bucket_index);
  uint64_t this_value_base_level = ValueFromIndex(bucket_index, sub_bucket_index);
  return this_value_base_level;
}


uint64_t HdrHistogram::HighestEquivalentValue(uint64_t value) const {
  return NextNonEquivalentValue(value) - 1;
}

uint64_t HdrHistogram::MedianEquivalentValue(uint64_t value) const {
  return (LowestEquivalentValue(value) + (SizeOfEquivalentValueRange(value) >> 1));
}

uint64_t HdrHistogram::NextNonEquivalentValue(uint64_t value) const {
  return LowestEquivalentValue(value) + SizeOfEquivalentValueRange(value);
}

bool HdrHistogram::ValuesAreEquivalent(uint64_t value1, uint64_t value2) const {
  return (LowestEquivalentValue(value1) == LowestEquivalentValue(value2));
}

uint64_t HdrHistogram::MinValue() const {
  return NoBarrier_Load(&min_value_);
}

uint64_t HdrHistogram::MaxValue() const {
  return NoBarrier_Load(&max_value_);
}

double HdrHistogram::MeanValue() const {
  RecordedValuesIterator iter(this);
  uint64_t total_value = 0;
  HistogramIterationValue val;
  while (iter.HasNext()) {
    Status s = iter.Next(&val);
    if (!s.ok()) {
      LOG(DFATAL) << "Error while iterating over histogram: " << s.ToString();
      return 0.0;
    }
    total_value = val.total_value_to_this_value;
  }
  return static_cast<double>(total_value) / TotalCount();
}

uint64_t HdrHistogram::ValueAtPercentile(double percentile) const {
  double requested_percentile = std::min(percentile, 100.0); // Truncate down to 100%
  uint64_t count_at_percentile =
    static_cast<uint64_t>(((requested_percentile / 100.0) * TotalCount()) + 0.5); // Round
  // Make sure we at least reach the first recorded entry
  count_at_percentile = std::max(count_at_percentile, 1LU);

  uint64_t total_to_current_iJ = 0;
  for (int i = 0; i < bucket_count_; i++) {
    int j = (i == 0) ? 0 : (sub_bucket_count_ / 2);
    for (; j < sub_bucket_count_; j++) {
      total_to_current_iJ += CountAt(i, j);
      if (total_to_current_iJ >= count_at_percentile) {
        uint64_t valueAtIndex = ValueFromIndex(i, j);
        return valueAtIndex;
      }
    }
  }

  LOG(DFATAL) << "Fell through while iterating, likely concurrent modification of histogram";
  return 0;
}

///////////////////////////////////////////////////////////////////////
// AbstractHistogramIterator
///////////////////////////////////////////////////////////////////////

AbstractHistogramIterator::AbstractHistogramIterator()
  : histogram_(NULL),
    cur_iter_val_(),
    saved_histogram_total_raw_count_(),
    current_bucket_index_(),
    current_sub_bucket_index_(),
    current_value_at_index_(),
    next_bucket_index_(),
    next_sub_bucket_index_(),
    next_value_at_index_(),
    prev_value_iterated_to_(),
    total_count_to_prev_index_(),
    total_count_to_current_index_(),
    total_value_to_current_index_(),
    array_total_count_(),
    count_at_this_value_(),
    fresh_sub_bucket_() {
}

void AbstractHistogramIterator::ResetIterator(const HdrHistogram* histogram) {
  this->histogram_ = histogram;
  this->saved_histogram_total_raw_count_ = histogram->TotalCount();
  this->array_total_count_ = histogram->TotalCount();
  this->current_bucket_index_ = 0;
  this->current_sub_bucket_index_ = 0;
  this->current_value_at_index_ = 0;
  this->next_bucket_index_ = 0;
  this->next_sub_bucket_index_ = 1;
  this->next_value_at_index_ = 1;
  this->prev_value_iterated_to_ = 0;
  this->total_count_to_prev_index_ = 0;
  this->total_count_to_current_index_ = 0;
  this->total_value_to_current_index_ = 0;
  this->count_at_this_value_ = 0;
  this->fresh_sub_bucket_ = true;
  cur_iter_val_.Reset();
}

bool AbstractHistogramIterator::HasNext() const {
  return total_count_to_current_index_ < array_total_count_;
}

Status AbstractHistogramIterator::Next(HistogramIterationValue* value) {
  // Sanity check.
  if (histogram_->TotalCount() != saved_histogram_total_raw_count_) {
    return Status::IllegalState("Concurrently modified histogram while traversing it");
  }

  // Move through the sub buckets and buckets until we hit the next  reporting level:
  while (!ExhaustedSubBuckets()) {
    count_at_this_value_ = histogram_->CountAt(current_bucket_index_, current_sub_bucket_index_);
    if (fresh_sub_bucket_) { // Don't add unless we've incremented since last bucket...
      total_count_to_current_index_ += count_at_this_value_;
      total_value_to_current_index_ +=
        count_at_this_value_ * histogram_->MedianEquivalentValue(current_value_at_index_);
      fresh_sub_bucket_ = false;
    }
    if (ReachedIterationLevel()) {
      uint64_t value_iterated_to = ValueIteratedTo();

      // Update iterator value.
      cur_iter_val_.value_iterated_to = value_iterated_to;
      cur_iter_val_.value_iterated_from = prev_value_iterated_to_;
      cur_iter_val_.count_at_value_iterated_to = count_at_this_value_;
      cur_iter_val_.count_added_in_this_iteration_step =
          (total_count_to_current_index_ - total_count_to_prev_index_);
      cur_iter_val_.total_count_to_this_value = total_count_to_current_index_;
      cur_iter_val_.total_value_to_this_value = total_value_to_current_index_;
      cur_iter_val_.percentile =
          ((100.0 * total_count_to_current_index_) / array_total_count_);
      cur_iter_val_.percentile_level_iterated_to = PercentileIteratedTo();

      prev_value_iterated_to_ = value_iterated_to;
      total_count_to_prev_index_ = total_count_to_current_index_;
      // Move the next percentile reporting level forward.
      IncrementIterationLevel();

      *value = cur_iter_val_;
      return Status::OK();
    }
    IncrementSubBucket();
  }
  return Status::IllegalState("Histogram array index out of bounds while traversing");
}

double AbstractHistogramIterator::PercentileIteratedTo() const {
  return (100.0 * static_cast<double>(total_count_to_current_index_)) / array_total_count_;
}

double AbstractHistogramIterator::PercentileIteratedFrom() const {
  return (100.0 * static_cast<double>(total_count_to_prev_index_)) / array_total_count_;
}

uint64_t AbstractHistogramIterator::ValueIteratedTo() const {
  return histogram_->HighestEquivalentValue(current_value_at_index_);
}

bool AbstractHistogramIterator::ExhaustedSubBuckets() const {
  return (current_bucket_index_ >= histogram_->bucket_count_);
}

void AbstractHistogramIterator::IncrementSubBucket() {
  fresh_sub_bucket_ = true;
  // Take on the next index:
  current_bucket_index_ = next_bucket_index_;
  current_sub_bucket_index_ = next_sub_bucket_index_;
  current_value_at_index_ = next_value_at_index_;
  // Figure out the next next index:
  next_sub_bucket_index_++;
  if (next_sub_bucket_index_ >= histogram_->sub_bucket_count_) {
    next_sub_bucket_index_ = histogram_->sub_bucket_half_count_;
    next_bucket_index_++;
  }
  next_value_at_index_ = HdrHistogram::ValueFromIndex(next_bucket_index_, next_sub_bucket_index_);
}

///////////////////////////////////////////////////////////////////////
// RecordedValuesIterator
///////////////////////////////////////////////////////////////////////

RecordedValuesIterator::RecordedValuesIterator(const HdrHistogram* histogram)
  : AbstractHistogramIterator(),
    visited_sub_bucket_index_(),
    visited_bucket_index_() {
  Reset(histogram);
}

void RecordedValuesIterator::Reset() {
  Reset(histogram_);
}

void RecordedValuesIterator::IncrementIterationLevel() {
  visited_sub_bucket_index_ = current_sub_bucket_index_;
  visited_bucket_index_ = current_bucket_index_;
}

bool RecordedValuesIterator::ReachedIterationLevel() const {
  uint64_t current_iJCount = histogram_->CountAt(current_bucket_index_, current_sub_bucket_index_);
  return (current_iJCount != 0) &&
    ((visited_sub_bucket_index_ != current_sub_bucket_index_) ||
     (visited_bucket_index_ != current_bucket_index_));
}

void RecordedValuesIterator::Reset(const HdrHistogram* histogram) {
  AbstractHistogramIterator::ResetIterator(histogram);
  visited_sub_bucket_index_ = -1;
  visited_bucket_index_ = -1;
}

///////////////////////////////////////////////////////////////////////
// PercentileIterator
///////////////////////////////////////////////////////////////////////

PercentileIterator::PercentileIterator(const HdrHistogram* histogram, int percentile_ticks_per_half_distance)
  : AbstractHistogramIterator(),
    percentile_ticks_per_half_distance_(percentile_ticks_per_half_distance),
    percentile_level_to_iterate_to_(),
    percentile_level_to_iterate_from_(),
    reached_last_recorded_value_(false) {
  Reset(histogram, percentile_ticks_per_half_distance);
}

void PercentileIterator::Reset(int percentile_ticks_per_half_distance) {
  Reset(histogram_, percentile_ticks_per_half_distance);
}

bool PercentileIterator::HasNext() const {
  if (AbstractHistogramIterator::HasNext()) {
    return true;
  }
  // We want one additional last step to 100%
  if (!reached_last_recorded_value_ && (array_total_count_ > 0)) {
    const_cast<PercentileIterator*>(this)->percentile_level_to_iterate_to_ = 100.0;
    const_cast<PercentileIterator*>(this)->reached_last_recorded_value_ = true;
    return true;
  }
  return false;
}

double PercentileIterator::PercentileIteratedTo() const {
  return percentile_level_to_iterate_to_;
}


double PercentileIterator::PercentileIteratedFrom() const {
  return percentile_level_to_iterate_from_;
}

void PercentileIterator::IncrementIterationLevel() {
  percentile_level_to_iterate_from_ = percentile_level_to_iterate_to_;
  // TODO: Can this expression be simplified?
  uint64_t percentile_reporting_ticks = percentile_ticks_per_half_distance_ *
    static_cast<uint64_t>(pow(2,
          static_cast<uint64_t>(log(100.0 / (100.0 - (percentile_level_to_iterate_to_))) / log(2)) + 1));
  percentile_level_to_iterate_to_ += 100.0 / percentile_reporting_ticks;
}

bool PercentileIterator::ReachedIterationLevel() const {
  if (count_at_this_value_ == 0) {
    return false;
  }
  double current_percentile =
      (100.0 * static_cast<double>(total_count_to_current_index_)) / array_total_count_;
  return (current_percentile >= percentile_level_to_iterate_to_);
}

void PercentileIterator::Reset(const HdrHistogram* histogram, int percentile_ticks_per_half_distance) {
  ResetIterator(histogram);
  percentile_ticks_per_half_distance_ = percentile_ticks_per_half_distance;
  percentile_level_to_iterate_to_ = 0.0;
  percentile_level_to_iterate_from_ = 0.0;
  reached_last_recorded_value_ = false;
}

} // namespace kudu
