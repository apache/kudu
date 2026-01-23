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

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <list>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

// IWYU pragma: no_include "testing/base/public/gunit.h"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h> // IWYU pragma: keep
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kudu/codegen/code_generator.h"
#include "kudu/codegen/compilation_manager.h"
#include "kudu/codegen/row_projector.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/singleton.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::array;
using std::atomic;
using std::back_inserter;
using std::list;
using std::sample;
using std::string;
using std::unique_ptr;
using std::thread;
using std::vector;

DEFINE_int32(codegen_test_random_schemas_runtime_sec, 60,
             "number of seconds to run the CodegenTest.CodegenRandomSchemas "
             "scenario; a negative number means 'unlimited'");

DECLARE_bool(codegen_dump_mc);
DECLARE_int32(codegen_cache_capacity);
DECLARE_int32(codegen_queue_capacity);
DECLARE_int32(codegen_compiler_manager_pool_max_threads_num);

namespace kudu {

typedef RowProjector NoCodegenRP;
typedef codegen::RowProjector CodegenRP;

using codegen::CompilationManager;

class CodegenTest : public KuduTest {
 public:
  CodegenTest()
    : random_(SeedRandom()),
      // Set the initial Arena size as small as possible to catch errors during relocation.
      projections_mem_(16) {
    // Create the base schema.
    vector<ColumnSchema> cols = {
        ColumnSchema("key           ", UINT64, ColumnSchema::NOT_NULL),
        ColumnSchema("int32         ",  INT32, ColumnSchema::NOT_NULL),
        ColumnSchema("int32-null-val",  INT32, ColumnSchema::NULLABLE),
        ColumnSchema("int32-null    ",  INT32, ColumnSchema::NULLABLE),
        ColumnSchema("str32         ", STRING, ColumnSchema::NOT_NULL),
        ColumnSchema("str32-null-val", STRING, ColumnSchema::NULLABLE),
        ColumnSchema("str32-null    ", STRING, ColumnSchema::NULLABLE),
    };
    CHECK_OK(base_.Reset(cols, 1));
    base_ = SchemaBuilder(base_).Build(); // add IDs

    // Create an extended default schema
    cols.emplace_back(ColumnSchemaBuilder()
                          .name("int32-R ")
                          .type(INT32)
                          .read_default(kI32R));
    cols.emplace_back(ColumnSchemaBuilder()
                          .name("int32-RW")
                          .type(INT32)
                          .read_default(kI32R)
                          .write_default(kI32W));
    cols.emplace_back(ColumnSchemaBuilder()
                          .name("str32-R ")
                          .type(STRING)
                          .read_default(kStrR));
    cols.emplace_back(ColumnSchemaBuilder()
                          .name("str32-RW")
                          .type(STRING)
                          .read_default(kStrR)
                          .write_default(kStrW));
    CHECK_OK(defaults_.Reset(cols, 1));
    defaults_ = SchemaBuilder(defaults_).Build(); // add IDs

    test_rows_arena_.reset(new Arena(2 * 1024));
    RowBuilder rb(&base_);
    for (int i = 0; i < kNumTestRows; ++i) {
      rb.AddUint64(i);
      rb.AddInt32(random_.Next32());
      rb.AddInt32(random_.Next32());
      rb.AddNull();
      AddRandomString(&rb);
      AddRandomString(&rb);
      rb.AddNull();

      void* arena_data = test_rows_arena_->AllocateBytes(
        ContiguousRowHelper::row_size(base_));
      ContiguousRow dst(&base_, static_cast<uint8_t*>(arena_data));
      CHECK_OK(CopyRow(rb.row(), &dst, test_rows_arena_.get()));
      test_rows_[i].reset(new ConstContiguousRow(dst));
      rb.Reset();
    }
  }

 protected:
  typedef const void* DefaultValueType;
  static const DefaultValueType kI8R;
  static const DefaultValueType kI8W;
  static const DefaultValueType kI16R;
  static const DefaultValueType kI16W;
  static const DefaultValueType kI32R;
  static const DefaultValueType kI32W;
  static const DefaultValueType kI64R;
  static const DefaultValueType kI64W;
  static const DefaultValueType kStrR;
  static const DefaultValueType kStrW;

  Schema base_;
  Schema defaults_;
  Random random_;

  // Compares the projection-for-read and projection-for-write results
  // of the codegen projection and the non-codegen projection
  template<bool READ>
  void TestProjection(const Schema* proj);
  // Generates a new row projector for the given projection schema.
  Status Generate(const Schema* proj, unique_ptr<CodegenRP>* out);

  enum {
    // Base schema column indices
    kKeyCol,
    kI32Col,
    kI32NullValCol,
    kI32NullCol,
    kStrCol,
    kStrNullValCol,
    kStrNullCol,
    // Extended default projection schema column indices
    kI32RCol,
    kI32RWCol,
    kStrRCol,
    kStrRWCol
  };

  Status CreatePartialSchema(const vector<size_t>& col_indexes,
                             Schema* out);

 private:
  // Projects the test rows into parameter rowblock using projector and
  // member projections_mem_ (should be Reset() manually).
  template<bool READ, class RowProjectorType>
  void ProjectTestRows(RowProjectorType* rp, RowBlock* rb);
  void AddRandomString(RowBuilder* rb);

  static const int kRandomStringMaxLength = 32;
  static const int kNumTestRows = 10;
  static const size_t kIndirectPerRow = 4 * kRandomStringMaxLength;
  static const size_t kIndirectPerProjection = kIndirectPerRow * kNumTestRows;

  codegen::CodeGenerator generator_;
  unique_ptr<ConstContiguousRow> test_rows_[kNumTestRows];
  RowBlockMemory projections_mem_;
  unique_ptr<Arena> test_rows_arena_;
};

namespace {

const int8_t kI8RValue = 0xBE;
const int8_t kI8WValue = 0xEB;
const int16_t kI16RValue = 0xA5A5;
const int16_t kI16WValue = 0x5A5A;
const int32_t kI32RValue = 0xFFFF0000;
const int32_t kI32WValue = 0x0000FFFF;
const int64_t kI64RValue = 0xF0F0F0F0F0F0F0F0;
const int64_t kI64WValue = 0x0F0F0F0F0F0F0F0F;
const Slice kStrRValue = "RRRRR STRING DEFAULT READ";
const Slice kStrWValue = "WWWWW STRING DEFAULT WRITE";

// Assumes all rows are selected
// Also assumes schemas are the same.
void CheckRowBlocksEqual(const RowBlock* rb1, const RowBlock* rb2,
                         const string& name1, const string& name2) {
  CHECK_EQ(rb1->nrows(), rb2->nrows());
  const Schema* schema = rb1->schema();
  for (int i = 0; i < rb1->nrows(); ++i) {
    RowBlockRow row1 = rb1->row(i);
    RowBlockRow row2 = rb2->row(i);
    CHECK_EQ(schema->Compare(row1, row2), 0)
      << "Rows unequal (failed at row " << i << "):\n"
      << "\t(" << name1 << ") = " << schema->DebugRow(row1) << "\n"
      << "\t(" << name2 << ") = " << schema->DebugRow(row2);
  }
}

} // anonymous namespace

const CodegenTest::DefaultValueType CodegenTest::kI8R = &kI8RValue;
const CodegenTest::DefaultValueType CodegenTest::kI8W = &kI8WValue;
const CodegenTest::DefaultValueType CodegenTest::kI16R = &kI16RValue;
const CodegenTest::DefaultValueType CodegenTest::kI16W = &kI16WValue;
const CodegenTest::DefaultValueType CodegenTest::kI32R = &kI32RValue;
const CodegenTest::DefaultValueType CodegenTest::kI32W = &kI32WValue;
const CodegenTest::DefaultValueType CodegenTest::kI64R = &kI64RValue;
const CodegenTest::DefaultValueType CodegenTest::kI64W = &kI64WValue;
const CodegenTest::DefaultValueType CodegenTest::kStrR = &kStrRValue;
const CodegenTest::DefaultValueType CodegenTest::kStrW = &kStrWValue;

void CodegenTest::AddRandomString(RowBuilder* rb) {
  static char buf[kRandomStringMaxLength];
  int size = random_.Uniform(kRandomStringMaxLength);
  RandomString(buf, size, &random_);
  rb->AddString(Slice(buf, size));
}

template<bool READ, class RowProjectorType>
void CodegenTest::ProjectTestRows(RowProjectorType* rp, RowBlock* rb) {
  // Even though we can test two rows at a time, without using up the
  // extra memory for keeping an entire row block around, this tests
  // what the actual use case will be.
  for (int i = 0; i < kNumTestRows; ++i) {
    ConstContiguousRow src = *test_rows_[i];
    RowBlockRow dst = rb->row(i);
    if (READ) {
      CHECK_OK(rp->ProjectRowForRead(src, &dst, rb->arena()));
    } else {
      CHECK_OK(rp->ProjectRowForWrite(src, &dst, rb->arena()));
    }
  }
}

template<bool READ>
void CodegenTest::TestProjection(const Schema* proj) {
  unique_ptr<CodegenRP> with;
  ASSERT_OK(Generate(proj, &with));
  NoCodegenRP without(&base_, proj);
  ASSERT_OK(without.Init());

  CHECK_EQ(with->base_schema(), &base_);
  CHECK_EQ(with->projection(), proj);

  RowBlock rb_with(proj, kNumTestRows, &projections_mem_);
  RowBlock rb_without(proj, kNumTestRows, &projections_mem_);

  projections_mem_.Reset();
  ProjectTestRows<READ>(with.get(), &rb_with);
  ProjectTestRows<READ>(&without, &rb_without);
  CheckRowBlocksEqual(&rb_with, &rb_without, "Codegen", "Expected");
}

Status CodegenTest::Generate(const Schema* proj, unique_ptr<CodegenRP>* out) {
  scoped_refptr<codegen::RowProjectorFunctions> functions;
  RETURN_NOT_OK(generator_.CompileRowProjector(base_, *proj, &functions));
  out->reset(new CodegenRP(&base_, proj, functions));
  return Status::OK();
}

Status CodegenTest::CreatePartialSchema(const vector<size_t>& col_indexes,
                                        Schema* out) {
  vector<ColumnId> col_ids;
  for (size_t col_idx : col_indexes) {
    col_ids.push_back(defaults_.column_id(col_idx));
  }
  return defaults_.CreateProjectionByIdsIgnoreMissing(col_ids, out);
}

TEST_F(CodegenTest, ObservablesTest) {
  // Test when not identity
  Schema proj = base_.CreateKeyProjection();
  unique_ptr<CodegenRP> with;
  CHECK_OK(Generate(&proj, &with));
  NoCodegenRP without(&base_, &proj);
  ASSERT_OK(without.Init());
  ASSERT_EQ(with->base_schema(), without.base_schema());
  ASSERT_EQ(with->projection(), without.projection());
  ASSERT_EQ(with->is_identity(), without.is_identity());
  ASSERT_FALSE(with->is_identity());

  // Test when identity
  Schema iproj = *&base_;
  unique_ptr<CodegenRP> iwith;
  CHECK_OK(Generate(&iproj, &iwith));
  NoCodegenRP iwithout(&base_, &iproj);
  ASSERT_OK(iwithout.Init());
  ASSERT_EQ(iwith->base_schema(), iwithout.base_schema());
  ASSERT_EQ(iwith->projection(), iwithout.projection());
  ASSERT_EQ(iwith->is_identity(), iwithout.is_identity());
  ASSERT_TRUE(iwith->is_identity());
}

// Test empty projection
TEST_F(CodegenTest, TestEmpty) {
  Schema empty;
  TestProjection<true>(&empty);
  TestProjection<false>(&empty);
}

// Test key projection
TEST_F(CodegenTest, TestKey) {
  Schema key = base_.CreateKeyProjection();
  TestProjection<true>(&key);
  TestProjection<false>(&key);
}

// Test int projection
TEST_F(CodegenTest, TestInts) {
  Schema ints;
  vector<size_t> part_cols = { kI32Col, kI32NullValCol, kI32NullCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &ints));

  TestProjection<true>(&ints);
  TestProjection<false>(&ints);
}

// Test string projection
TEST_F(CodegenTest, TestStrings) {
  Schema strs;
  vector<size_t> part_cols = { kStrCol, kStrNullValCol, kStrNullCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &strs));

  TestProjection<true>(&strs);
  TestProjection<false>(&strs);
}

// Tests the projection of every non-nullable column
TEST_F(CodegenTest, TestNonNullables) {
  Schema non_null;
  vector<size_t> part_cols = { kKeyCol, kI32Col, kStrCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &non_null));

  TestProjection<true>(&non_null);
  TestProjection<false>(&non_null);
}

// Tests the projection of every nullable column
TEST_F(CodegenTest, TestNullables) {
  Schema nullables;
  vector<size_t> part_cols = { kI32NullValCol, kI32NullCol, kStrNullValCol, kStrNullCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &nullables));

  TestProjection<true>(&nullables);
  TestProjection<false>(&nullables);
}

// Test full schema projection
TEST_F(CodegenTest, TestFullSchema) {
  TestProjection<true>(&base_);
  TestProjection<false>(&base_);
}

// Tests just the default projection
TEST_F(CodegenTest, TestDefaultsOnly) {
  Schema pure_defaults;

  // Default read projections
  vector<size_t> part_cols = { kI32RCol, kI32RWCol, kStrRCol, kStrRWCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &pure_defaults));

  TestProjection<true>(&pure_defaults);

  // Default write projections
  part_cols = { kI32RWCol, kStrRWCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &pure_defaults));

  TestProjection<false>(&pure_defaults);
}

// Test full defaults projection
TEST_F(CodegenTest, TestFullSchemaWithDefaults) {
  TestProjection<true>(&defaults_);

  // Default write projection
  Schema full_write;
  vector<size_t> part_cols = { kKeyCol,
                               kI32Col,
                               kI32NullValCol,
                               kI32NullCol,
                               kStrCol,
                               kStrNullValCol,
                               kStrNullCol,
                               kI32RWCol,
                               kStrRWCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &full_write));

  TestProjection<false>(&full_write);
}

// Test the codegen_dump_mc flag works properly.
TEST_F(CodegenTest, TestDumpMC) {
  FLAGS_codegen_dump_mc = true;

  StringVectorSink sink;
  ScopedRegisterSink srs(&sink);

  Schema ints;
  vector<size_t> part_cols = { kI32Col, kI32NullValCol, kI32NullCol, kStrCol };
  ASSERT_OK(CreatePartialSchema(part_cols, &ints));
  TestProjection<true>(&ints);

  const vector<string>& msgs = sink.logged_msgs();
  ASSERT_EQ(msgs.size(), 1);
#if defined(__powerpc__)
  EXPECT_THAT(msgs[0], testing::ContainsRegex("blr"));
#elif defined(__aarch64__)
  EXPECT_THAT(msgs[0], testing::ContainsRegex("ret"));
#else
  EXPECT_THAT(msgs[0], testing::ContainsRegex("retq"));
#endif  // #if defined(__powerpc__) ... #elif defined(__aarch64__) ... #else ...
}

// Basic test for the CompilationManager code cache.
// This runs a bunch of compilation tasks and ensures that the cache
// sometimes hits on the second attempt for the same projection.
TEST_F(CodegenTest, TestCodeCache) {
  Singleton<CompilationManager>::UnsafeReset();
  FLAGS_codegen_cache_capacity = 10;
  CompilationManager* cm = CompilationManager::GetSingleton();

  for (int pass = 0; pass < 2; pass++) {
    int num_hits = 0;

    // Generate all permutations of the first four columns (24 permutations).
    // For each such permutation, we'll create a projection and request code generation.
    vector<size_t> perm = { 0, 1, 2, 3 };
    do {
      SCOPED_TRACE(perm);
      Schema projection;
      ASSERT_OK(CreatePartialSchema(perm, &projection));

      unique_ptr<CodegenRP> projector;
      if (cm->RequestRowProjector(&base_, &projection, &projector)) {
        num_hits++;
      }
      cm->Wait();
    } while (std::next_permutation(perm.begin(), perm.end()));

    if (pass == 0) {
      // On the first pass, the cache should have been empty and gotten 0 hits.
      ASSERT_EQ(0, num_hits);
    } else {
      // Otherwise, we expect to have gotten some hits.
      // If our cache were a perfect LRU implementation, then we would actually
      // expect 0 hits here as well, since we are accessing the entries in
      // exactly the same order as we inserted them, and thus would evict
      // an entry before we look for it again. But, our LRU cache is sharded
      // so we expect to get some hits on the second time.
      ASSERT_GT(num_hits, 0);
      ASSERT_LT(num_hits, 24);
    }
  }
}

// Test scenario to reproduce the race condition that leads to the behavior
// described by KUDU-3545 before it was addressed. This works accross different
// compilers/toolchains, while KUDU-3545 originally stipulated that the issue
// is specific to GCC13 and the related toolchain.
TEST_F(CodegenTest, CodegenEHFrameRace) {
  constexpr const size_t kNumThreads = 2;

  // For easier reproduction of the race, allow for multiple threads in the
  // codegen compile thread pool. It results in a race condition in libgcc
  // when concurrently registering EH frames.
  FLAGS_codegen_compiler_manager_pool_max_threads_num = 3;

  // A smaller codegen cache might help to create situations when a race
  // condition happens when just a single codegen compiler thread is active.
  // Upon adding a new entry into the cache, an entry would be evicted
  // if it's not enough space, and its EH frames would unregistered,
  // while another compiler request might be running at the only active thread
  // in the compiler thread pool, registering correspoding EH frames.
  // It creates a race condition in libgcc, and that's the original issue
  // behind KUDU-3545.
  FLAGS_codegen_cache_capacity = 10;
  Singleton<CompilationManager>::UnsafeReset();
  CompilationManager* cm = CompilationManager::GetSingleton();

  CountDownLatch start(kNumThreads);
  vector<thread> threads;
  threads.reserve(kNumThreads);
  for (size_t idx = 0; idx < kNumThreads; ++idx) {
    threads.emplace_back([&]() {
      start.Wait();
      for (auto pass = 0; pass < 10; ++pass) {
        // Generate all permutations of the first 4 columns (24 permutations).
        // For each such permutation, reate a projection and request code
        // generation.
        vector<size_t> perm = { 0, 1, 2, 4 };
        do {
          SCOPED_TRACE(perm);
          Schema projection;
          const auto s = CreatePartialSchema(perm, &projection);
          if (!s.ok()) {
            LOG(WARNING) << s.ToString();
            return;
          }

          unique_ptr<CodegenRP> projector;
          cm->RequestRowProjector(&base_, &projection, &projector);
        } while (std::next_permutation(perm.begin(), perm.end()));
      }
    });
  }
  start.CountDown(kNumThreads);

  for (auto& t : threads) {
    t.join();
  }
}

// This is a scenario to stress-test the generation of row projections' code.
// The generated code isn't run, but just compiled and put into the codegen
// cache. References to the generated code are being retained not only in the
// cache but also kept around for time intervals of randomized durations.
// The number and the order of columns in schemas and corresponding projections
// are randomized as well.
TEST_F(CodegenTest, CodegenRandomSchemas) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr const size_t kNumThreads = 2;
  constexpr const size_t kNumProjectionsPerSchema = 32;
  constexpr const size_t kNumShufflesPerProjection = 16;

  // Create 'library' of columns to build schemas for the test scenario.
  const array<ColumnSchema, 31> cs_library{
      ColumnSchema("key", INT64, ColumnSchema::NOT_NULL),
      ColumnSchema("c_bool", BOOL, ColumnSchema::NOT_NULL),
      ColumnSchema("c_bool_n", BOOL, ColumnSchema::NULLABLE),
      ColumnSchema("c_int8", INT8, ColumnSchema::NOT_NULL),
      ColumnSchema("c_int8_n", INT8, ColumnSchema::NULLABLE),
      ColumnSchema("c_int16", INT16, ColumnSchema::NOT_NULL),
      ColumnSchema("c_int16_n", INT16, ColumnSchema::NULLABLE),
      ColumnSchema("c_int32", INT32, ColumnSchema::NOT_NULL),
      ColumnSchema("c_int32_n", INT32, ColumnSchema::NULLABLE),
      ColumnSchema("c_int64", INT64, ColumnSchema::NOT_NULL),
      ColumnSchema("c_int64_n", INT64, ColumnSchema::NULLABLE),
      ColumnSchema("c_str", STRING, ColumnSchema::NOT_NULL),
      ColumnSchema("c_str_n", STRING, ColumnSchema::NULLABLE),
      ColumnSchema("c_bin", BINARY, ColumnSchema::NOT_NULL),
      ColumnSchema("c_bin_n", BINARY, ColumnSchema::NULLABLE),
      ColumnSchemaBuilder()
          .name("c_int32_r")
          .type(INT32)
          .read_default(kI32R),
      ColumnSchemaBuilder()
          .name("c_int32_nr")
          .type(INT32)
          .nullable(true)
          .read_default(kI32R),
      ColumnSchemaBuilder()
          .name("c_int32_rw")
          .type(INT32)
          .read_default(kI32R)
          .write_default(kI32W),
      ColumnSchemaBuilder()
          .name("c_int32_nrw")
          .type(INT32)
          .nullable(true)
          .read_default(kI32R)
          .write_default(kI32W),
      ColumnSchemaBuilder()
          .name("c_int64_r")
          .type(INT64)
          .read_default(kI64R),
      ColumnSchemaBuilder()
          .name("c_int64_nr")
          .type(INT64)
          .nullable(true)
          .read_default(kI64R),
      ColumnSchemaBuilder()
          .name("c_int64_rw")
          .type(INT64)
          .read_default(kI64R)
          .write_default(kI64W),
      ColumnSchemaBuilder()
          .name("c_int64_nrw")
          .type(INT64)
          .nullable(true)
          .read_default(kI64R)
          .write_default(kI64W),
      ColumnSchemaBuilder()
          .name("c_str_r")
          .type(STRING)
          .read_default(kStrR),
      ColumnSchemaBuilder()
          .name("c_str_nr")
          .type(STRING)
          .nullable(true)
          .read_default(kStrR),
      ColumnSchemaBuilder()
          .name("c_str_rw")
          .type(STRING)
          .read_default(kStrR)
          .write_default(kStrW),
      ColumnSchemaBuilder()
          .name("c_str_nrw")
          .type(STRING)
          .nullable(true)
          .read_default(kStrR)
          .write_default(kStrW),
      ColumnSchemaBuilder()
          .name("c_bin_r")
          .type(BINARY)
          .read_default(kStrR),
      ColumnSchemaBuilder()
          .name("c_bin_nr")
          .type(BINARY)
          .nullable(true)
          .read_default(kStrR),
      ColumnSchemaBuilder()
          .name("c_bin_rw")
          .type(BINARY)
          .read_default(kStrR)
          .write_default(kStrW),
      ColumnSchemaBuilder()
          .name("c_bin_nrw")
          .type(BINARY)
          .nullable(true)
          .read_default(kStrR)
          .write_default(kStrW),
  };

  // A part of the codegenned projection code should fit into the cache, but
  // make sure the elements are purged out of the cache from time to time.
  FLAGS_codegen_cache_capacity =
      kNumThreads * kNumProjectionsPerSchema * kNumShufflesPerProjection / 5;

  // Make sure there is enough space in the codegen compilation queue to
  // accommodate requests from all the running threads. Even if each of them
  // waits for all currently running compilations to complete, they might
  // race to submit their tasks, so add significant extra margin.
  FLAGS_codegen_queue_capacity = 3 * kNumThreads;

  Singleton<CompilationManager>::UnsafeReset();
  CompilationManager* cm = CompilationManager::GetSingleton();

  atomic<bool> stop = false;
  vector<thread> threads;
  threads.reserve(kNumThreads);
  for (size_t thread_idx = 0; thread_idx < kNumThreads; ++thread_idx) {
    threads.emplace_back([&, thread_idx = thread_idx]() {
      std::mt19937 gen(SeedRandom());
      list<unique_ptr<CodegenRP>> projectors;
      while (!stop) {
        // Choose a random number: it's the number of columns in the new schema.
        const size_t num_columns = 1 + gen() % cs_library.size();
        VLOG(1) << StringPrintf("thread %2zd: %2zd-column schema",
                                  thread_idx, num_columns);
        auto cs_library_first_non_key = cs_library.begin();
        ++cs_library_first_non_key;
        vector<ColumnSchema> column_schemas;
        column_schemas.reserve(num_columns);
        column_schemas.push_back(cs_library[0]); // the 'key' column is always present
        sample(cs_library_first_non_key,
               cs_library.end(),
               back_inserter(column_schemas),
               num_columns - 1,
               gen);

        // Create a schema with the given number of columns, picking columns
        // from the 'column schema library' in random order.
        SchemaBuilder sb;
        for (const auto& cs : column_schemas) {
          CHECK_OK(sb.AddColumn(cs));
        }
        const Schema schema = sb.Build();

        // Generate various projections, using random subsets of columns
        // in the schema.
        for (size_t iter = 0; iter < kNumProjectionsPerSchema && !stop; ++iter) {
          const size_t proj_col_num = 1 + gen() % column_schemas.size();
          VLOG(2) << StringPrintf("thread %2zd: %2zd-column projection",
                                  thread_idx, proj_col_num);
          vector<ColumnId> col_ids;
          col_ids.reserve(proj_col_num);

          const auto& all_col_ids = schema.column_ids();
          sample(all_col_ids.begin(),
                 all_col_ids.end(),
                 back_inserter(col_ids),
                 proj_col_num,
                 gen);
          for (size_t s_idx = 0; s_idx < kNumShufflesPerProjection; ++s_idx) {
            Schema projection;
            // Shuffle the contents since std::sample is stable with the given
            // vector's forward iterator.
            if (s_idx != 0) {
              std::shuffle(col_ids.begin(), col_ids.end(), gen);
            }

            // Create a projection with columns at the specified indices.
            CHECK_OK(schema.CreateProjectionByIdsIgnoreMissing(col_ids, &projection));

            // Request code generation.
            unique_ptr<CodegenRP> projector;
            if (cm->RequestRowProjector(&schema, &projection, &projector)) {
              // If that's a codegenned projector, store it to keep a reference.
              projectors.push_back(std::move(projector));
            } else {
              // Let the codegen compilation complete before going next cycle to
              // avoid overflowing the codegen queue.
              cm->Wait();
            }
          }

          // Randomly purge some of the references to keep at most
          // kNumProjectionsPerSchema between cycles in the 'projections'
          // container.
          size_t count = 0;
          while (projectors.size() > kNumProjectionsPerSchema / 2) {
            const size_t offset = gen() % projectors.size();
            auto it = projectors.cbegin();
            std::advance(it, offset);
            projectors.erase(it);
            ++count;
          }
          VLOG(2) << StringPrintf("thread %2zd: %4zd references dropped",
                                  thread_idx, count);
        }
      }
    });
  }

  // Let them run for at least for the specified time.
  if (const int32 runtime_sec = FLAGS_codegen_test_random_schemas_runtime_sec;
      runtime_sec >= 0) {
    SleepFor(MonoDelta::FromSeconds(runtime_sec));
    stop = true;
  }
  for (auto& t : threads) {
    t.join();
  }
}

} // namespace kudu
