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

#include "kudu/client/scan_predicate.h"

#include <memory>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/client/hash-internal.h"
#include "kudu/client/scan_predicate-internal.h"
#include "kudu/client/value-internal.h"
#include "kudu/client/value.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/block_bloom_filter.h"
#include "kudu/util/status.h"

using boost::optional;
using std::move;
using std::shared_ptr;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

class Arena;

namespace client {

KuduPredicate::KuduPredicate(Data* d)
  : data_(d) {
}

KuduPredicate::~KuduPredicate() {
  delete data_;
}

KuduPredicate::Data::Data() {
}

KuduPredicate::Data::~Data() {
}

KuduPredicate* KuduPredicate::Clone() const {
  return new KuduPredicate(data_->Clone());
}

ComparisonPredicateData::ComparisonPredicateData(ColumnSchema col,
                                                 KuduPredicate::ComparisonOp op,
                                                 KuduValue* val)
    : col_(move(col)),
      op_(op),
      val_(val) {
}

ComparisonPredicateData::~ComparisonPredicateData() {
}

Status ComparisonPredicateData::AddToScanSpec(ScanSpec* spec, Arena* arena) {
  void* val_void;
  RETURN_NOT_OK(val_->data_->CheckTypeAndGetPointer(col_.name(),
                                                    col_.type_info()->type(),
                                                    col_.type_attributes(),
                                                    &val_void));
  switch (op_) {
    case KuduPredicate::LESS_EQUAL: {
      optional<ColumnPredicate> pred =
        ColumnPredicate::InclusiveRange(col_, nullptr, val_void, arena);
      if (pred) {
        spec->AddPredicate(*pred);
      }
      break;
    };
    case KuduPredicate::GREATER_EQUAL: {
      spec->AddPredicate(ColumnPredicate::Range(col_, val_void, nullptr));
      break;
    };
    case KuduPredicate::EQUAL: {
      spec->AddPredicate(ColumnPredicate::Equality(col_, val_void));
      break;
    };
    case KuduPredicate::LESS: {
      spec->AddPredicate(ColumnPredicate::Range(col_, nullptr, val_void));
      break;
    };
    case KuduPredicate::GREATER: {
      optional<ColumnPredicate> pred =
        ColumnPredicate::ExclusiveRange(col_, val_void, nullptr, arena);
      if (pred) {
        spec->AddPredicate(*pred);
      }
      break;
    };
    default:
      return Status::InvalidArgument(Substitute("invalid comparison op: $0", op_));
  }
  return Status::OK();
}

InListPredicateData::InListPredicateData(ColumnSchema col,
                                         vector<KuduValue*>* values)
    : col_(move(col)) {
  vals_.swap(*values);
}

InListPredicateData::~InListPredicateData() {
  STLDeleteElements(&vals_);
}

Status InListPredicateData::AddToScanSpec(ScanSpec* spec, Arena* /*arena*/) {
  vector<const void*> vals_list;
  vals_list.reserve(vals_.size());
  for (auto value : vals_) {
    void* val_void;
    // The local vals_ list consists of KuduValue pointers that make up the IN
    // list predicate. For every value in the vals_ list a call to
    // CheckTypeAndGetPointer is made to get a local pointer (void*) to the
    // underlying value. The local list (vals_list) of all the void* pointers is
    // passed to the ColumnPredicate::InList constructor. The constructor for
    // ColumnPredicate::InList will assume ownership of the pointers via a swap.
    RETURN_NOT_OK(value->data_->CheckTypeAndGetPointer(col_.name(),
                                                       col_.type_info()->type(),
                                                       col_.type_attributes(),
                                                       &val_void));
    vals_list.push_back(val_void);
  }

  spec->AddPredicate(ColumnPredicate::InList(col_, &vals_list));

  return Status::OK();
}

// Helper function to add Bloom filters of different types to the scan spec.
// "func" is a functor that provides access to the underlying BlockBloomFilter ptr.
template<typename BloomFilterType, typename BloomFilterPtrFuncType>
static Status AddBloomFiltersToScanSpec(const ColumnSchema& col, ScanSpec* spec,
                                        const vector<BloomFilterType>& bloom_filters,
                                        BloomFilterPtrFuncType func) {
  // Extract the BlockBloomFilters.
  vector<BlockBloomFilter*> block_bloom_filters;
  block_bloom_filters.reserve(bloom_filters.size());
  for (const auto& bf : bloom_filters) {
    block_bloom_filters.push_back(func(bf));
  }

  spec->AddPredicate(ColumnPredicate::InBloomFilter(col, std::move(block_bloom_filters)));
  return Status::OK();
}

Status InBloomFilterPredicateData::AddToScanSpec(ScanSpec* spec, Arena* /*arena*/) {
  return AddBloomFiltersToScanSpec(col_, spec, bloom_filters_,
                                   [](const unique_ptr<KuduBloomFilter>& bf) {
                                     return bf->data_->bloom_filter_.get();
                                   });
}

InBloomFilterPredicateData* client::InBloomFilterPredicateData::Clone() const {
  vector<unique_ptr<KuduBloomFilter>> bloom_filter_clones;
  bloom_filter_clones.reserve(bloom_filters_.size());
  for (const auto& bf : bloom_filters_) {
    bloom_filter_clones.emplace_back(bf->Clone());
  }

  return new InBloomFilterPredicateData(col_, std::move(bloom_filter_clones));
}

Status InDirectBloomFilterPredicateData::AddToScanSpec(ScanSpec* spec, Arena* /*arena*/) {
  return AddBloomFiltersToScanSpec(col_, spec, bloom_filters_,
                                   [](const DirectBlockBloomFilterUniqPtr& bf) {
                                     return bf.get();
                                   });
}

InDirectBloomFilterPredicateData* InDirectBloomFilterPredicateData::Clone() const {
  // In the clone case, the objects are owned by InDirectBloomFilterPredicateData
  // and hence use the default deleter with shared_ptr.
  shared_ptr<BlockBloomFilterBufferAllocatorIf> allocator_clone =
      CHECK_NOTNULL(allocator_->Clone());

  vector<DirectBlockBloomFilterUniqPtr> bloom_filter_clones;
  bloom_filter_clones.reserve(bloom_filters_.size());
  for (const auto& bf : bloom_filters_) {
    unique_ptr<BlockBloomFilter> bf_clone;
    CHECK_OK(bf->Clone(allocator_clone.get(), &bf_clone));

    // Similarly for unique_ptr, specify that the BlockBloomFilter is owned by
    // InDirectBloomFilterPredicateData.
    DirectBlockBloomFilterUniqPtr direct_bf_clone(
        bf_clone.release(), DirectBloomFilterDataDeleter<BlockBloomFilter>(true /*owned*/));
    bloom_filter_clones.emplace_back(std::move(direct_bf_clone));
  }
  return new InDirectBloomFilterPredicateData(col_, std::move(allocator_clone),
                                              std::move(bloom_filter_clones));
}

KuduBloomFilter::KuduBloomFilter()  {
  data_ = new Data();
}

KuduBloomFilter::KuduBloomFilter(Data* other_data) :
   data_(CHECK_NOTNULL(other_data)) {
}

KuduBloomFilter::~KuduBloomFilter() {
  delete data_;
}

KuduBloomFilter* KuduBloomFilter::Clone() const {
  unique_ptr<Data> data_clone = data_->Clone();
  return new KuduBloomFilter(data_clone.release());
}

void KuduBloomFilter::Insert(const Slice& key) {
  DCHECK_NOTNULL(data_->bloom_filter_)->Insert(key);
}

KuduBloomFilterBuilder::Data::Data() :
    num_keys_(0),
    false_positive_probability_(0.01),
    hash_algorithm_(FAST_HASH),
    hash_seed_(0) {
}

KuduBloomFilterBuilder::KuduBloomFilterBuilder(size_t num_keys) {
  data_ = new Data;
  data_->num_keys_ = num_keys;
}

KuduBloomFilterBuilder::~KuduBloomFilterBuilder() {
  delete data_;
}

KuduBloomFilterBuilder& KuduBloomFilterBuilder::false_positive_probability(double fpp) {
  data_->false_positive_probability_ = fpp;
  return *this;
}

KuduBloomFilterBuilder& KuduBloomFilterBuilder::hash_algorithm(HashAlgorithm hash_algorithm) {
  data_->hash_algorithm_ = hash_algorithm;
  return *this;
}

KuduBloomFilterBuilder& KuduBloomFilterBuilder::hash_seed(uint32_t hash_seed) {
  data_->hash_seed_ = hash_seed;
  return *this;
}

Status KuduBloomFilterBuilder::Build(KuduBloomFilter** bloom_filter_out) {
  unique_ptr<KuduBloomFilter> bf(new KuduBloomFilter());
  bf->data_->allocator_ = DefaultBlockBloomFilterBufferAllocator::GetSingletonSharedPtr();
  bf->data_->bloom_filter_ = unique_ptr<BlockBloomFilter>(
      new BlockBloomFilter(bf->data_->allocator_.get()));

  int log_space_bytes = BlockBloomFilter::MinLogSpace(data_->num_keys_,
                                                      data_->false_positive_probability_);
  RETURN_NOT_OK(bf->data_->bloom_filter_->Init(
      log_space_bytes, ToInternalHashAlgorithm(data_->hash_algorithm_), data_->hash_seed_));

  *bloom_filter_out = bf.release();
  return Status::OK();
}

KuduBloomFilter::Data::Data(shared_ptr<BlockBloomFilterBufferAllocatorIf> allocator,
                            unique_ptr<BlockBloomFilter> bloom_filter) :
    allocator_(std::move(allocator)),
    bloom_filter_(std::move(bloom_filter)) {
}

unique_ptr<KuduBloomFilter::Data> KuduBloomFilter::Data::Clone() const {
  shared_ptr<BlockBloomFilterBufferAllocatorIf> allocator_clone =
      CHECK_NOTNULL(allocator_->Clone());
  unique_ptr<BlockBloomFilter> bloom_filter_clone;
  CHECK_OK(bloom_filter_->Clone(allocator_clone.get(), &bloom_filter_clone));

  return unique_ptr<KuduBloomFilter::Data>(
      new Data(std::move(allocator_clone), std::move(bloom_filter_clone)));
}

} // namespace client
} // namespace kudu
