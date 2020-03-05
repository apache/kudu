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
#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "kudu/client/scan_predicate.h"
#include "kudu/client/value-internal.h"
#include "kudu/client/value.h"
#include "kudu/common/scan_spec.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

class KuduPredicate::Data {
 public:
  Data();
  virtual ~Data();
  virtual Status AddToScanSpec(ScanSpec* spec, Arena* arena) = 0;
  virtual Data* Clone() const = 0;
};

// A predicate implementation which represents an error constructing
// some other predicate.
//
// This allows us to provide a simple API -- if a predicate fails to
// construct, we return an instance of this class instead of the requested
// predicate implementation. Then, when the caller adds it to a scanner,
// the error is returned.
class ErrorPredicateData : public KuduPredicate::Data {
 public:
  explicit ErrorPredicateData(Status s)
      : status_(std::move(s)) {
  }

  virtual ~ErrorPredicateData() {
  }

  Status AddToScanSpec(ScanSpec* spec, Arena* arena) override {
    return status_;
  }

  ErrorPredicateData* Clone() const override {
    return new ErrorPredicateData(status_);
  }

 private:
  Status status_;
};


// A simple binary comparison predicate between a column and
// a constant.
class ComparisonPredicateData : public KuduPredicate::Data {
 public:
  ComparisonPredicateData(ColumnSchema col,
                          KuduPredicate::ComparisonOp op,
                          KuduValue* value);
  virtual ~ComparisonPredicateData();

  Status AddToScanSpec(ScanSpec* spec, Arena* arena) override;

  ComparisonPredicateData* Clone() const override {
    return new ComparisonPredicateData(col_, op_, val_->Clone());
  }

 private:
  friend class KuduScanner;

  ColumnSchema col_;
  KuduPredicate::ComparisonOp op_;
  std::unique_ptr<KuduValue> val_;
};

// An InBloomFilter predicate for selecting values present in the vector of Bloom filters.
class InBloomFilterPredicateData : public KuduPredicate::Data {
 public:
  InBloomFilterPredicateData(ColumnSchema col,
                             std::vector<std::unique_ptr<KuduBloomFilter>> bloom_filters)
      : col_(std::move(col)),
        bloom_filters_(std::move(bloom_filters)) {
  }

  Status AddToScanSpec(ScanSpec* spec, Arena* arena) override;

  InBloomFilterPredicateData* Clone() const override;

 private:
  ColumnSchema col_;
  std::vector<std::unique_ptr<KuduBloomFilter>> bloom_filters_;
};

// A list predicate for a column and a list of constant values.
class InListPredicateData : public KuduPredicate::Data {
 public:
  InListPredicateData(ColumnSchema col, std::vector<KuduValue*>* values);

  virtual ~InListPredicateData();

  Status AddToScanSpec(ScanSpec* spec, Arena* arena) override;

  InListPredicateData* Clone() const override {
    std::vector<KuduValue*> values;
    values.reserve(vals_.size());
    for (KuduValue* val : vals_) {
      values.push_back(val->Clone());
    }

    return new InListPredicateData(col_, &values);
  }

 private:
  friend class KuduScanner;

  ColumnSchema col_;
  std::vector<KuduValue*> vals_;
};

// A predicate for selecting non-null values.
class IsNotNullPredicateData : public KuduPredicate::Data {
 public:
  explicit IsNotNullPredicateData(ColumnSchema col)
      : col_(std::move(col)) {
  }

  Status AddToScanSpec(ScanSpec* spec, Arena* /*arena*/) override {
    spec->AddPredicate(ColumnPredicate::IsNotNull(col_));
    return Status::OK();
  }

  IsNotNullPredicateData* Clone() const override {
    return new IsNotNullPredicateData(col_);
  }

 private:
  friend class KuduScanner;

  ColumnSchema col_;
};

// A predicate for selecting null values.
class IsNullPredicateData : public KuduPredicate::Data {
 public:
  explicit IsNullPredicateData(ColumnSchema col)
      : col_(std::move(col)) {
  }

  Status AddToScanSpec(ScanSpec* spec, Arena* /*arena*/) override {
    spec->AddPredicate(ColumnPredicate::IsNull(col_));
    return Status::OK();
  }

  IsNullPredicateData* Clone() const override {
    return new IsNullPredicateData(col_);
  }

 private:
  friend class KuduScanner;

  ColumnSchema col_;
};

class KuduBloomFilterBuilder::Data {
 public:
  Data();
  ~Data() = default;

  size_t num_keys_;
  double false_positive_probability_;
  HashAlgorithm hash_algorithm_;
  uint32_t hash_seed_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Data);
};

class KuduBloomFilter::Data {
 public:
  Data() = default;
  ~Data() = default;
  std::unique_ptr<Data> Clone() const;

  std::shared_ptr<BlockBloomFilterBufferAllocatorIf> allocator_;
  std::unique_ptr<BlockBloomFilter> bloom_filter_;

 private:
  Data(std::shared_ptr<BlockBloomFilterBufferAllocatorIf> allocator,
       std::unique_ptr<BlockBloomFilter> bloom_filter);

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu
