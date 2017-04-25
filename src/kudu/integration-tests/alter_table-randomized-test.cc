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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "kudu/client/client-test-util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/test_util.h"

namespace kudu {

using client::KuduClient;
using client::KuduColumnSchema;
using client::KuduColumnStorageAttributes;
using client::KuduError;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using client::KuduSession;
using client::KuduTable;
using client::KuduTableAlterer;
using client::KuduTableCreator;
using client::KuduValue;
using client::KuduWriteOperation;
using client::sp::shared_ptr;
using std::make_pair;
using std::map;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::SubstituteAndAppend;

const char* kTableName = "test-table";
const int kMaxColumns = 30;
const uint32_t kMaxRangePartitions = 32;
const vector<KuduColumnStorageAttributes::CompressionType> kCompressionTypes =
    { KuduColumnStorageAttributes::NO_COMPRESSION,
      KuduColumnStorageAttributes::SNAPPY,
      KuduColumnStorageAttributes::LZ4,
      KuduColumnStorageAttributes::ZLIB };
const vector <KuduColumnStorageAttributes::EncodingType> kInt32Encodings =
    { KuduColumnStorageAttributes::PLAIN_ENCODING,
      KuduColumnStorageAttributes::RLE,
      KuduColumnStorageAttributes::BIT_SHUFFLE };
// A block size of 0 applies the server-side default.
const vector<int32_t> kBlockSizes = {0, 2 * 1024 * 1024,
                                     4 * 1024 * 1024, 8 * 1024 * 1024};

class AlterTableRandomized : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    // This test produces tables with lots of columns. With container preallocation,
    // we end up using quite a bit of disk space. So, we disable it.
    opts.extra_tserver_flags.push_back("--log_container_preallocate_bytes=0");
    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());

    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

  void TearDown() override {
    cluster_->Shutdown();
    KuduTest::TearDown();
  }

  void RestartTabletServer(int idx) {
    LOG(INFO) << "Restarting TS " << idx;
    cluster_->tablet_server(idx)->Shutdown();
    CHECK_OK(cluster_->tablet_server(idx)->Restart());
    CHECK_OK(cluster_->WaitForTabletsRunning(cluster_->tablet_server(idx),
                                             -1, MonoDelta::FromSeconds(60)));
    LOG(INFO) << "TS " << idx << " Restarted";
  }

  void RestartMaster() {
    LOG(INFO) << "Restarting Master";
    cluster_->master()->Shutdown();
    CHECK_OK(cluster_->master()->Restart());
    CHECK_OK(cluster_->master()->WaitForCatalogManager());
    CHECK_OK(cluster_->WaitForTabletServerCount(3, MonoDelta::FromSeconds(60)));
    LOG(INFO) << "Master Restarted";
  }

 protected:
  unique_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
};

struct RowState {
  // We use this special value to denote NULL values.
  // We ensure that we never insert or update to this value except in the case of
  // NULLable columns.
  static const int32_t kNullValue = 0xdeadbeef;
  // We use this special value to denote default values.
  // We ensure that we never insert or update to this value except when the
  // column should be assigned its current default value.
  static const int32_t kDefaultValue = 0xbabecafe;
  vector<pair<string, int32_t>> cols;

  string ToString() const {
    string ret = "(";
    bool first = true;
    for (const auto& e : cols) {
      if (!first) {
        ret.append(", ");
      }
      first = false;
      if (e.second == kNullValue) {
        SubstituteAndAppend(&ret, "int32 $0=$1", e.first, "NULL");
      } else {
        SubstituteAndAppend(&ret, "int32 $0=$1", e.first, e.second);
      }
    }
    ret.push_back(')');
    return ret;
  }
};

struct TableState {
  TableState()
      : rand_(SeedRandom()) {
    col_names_.push_back("key");
    col_nullable_.push_back(false);
    col_defaults_.push_back(0);
    AddRangePartition();
  }

  int32_t GetRandomNewRowKey() {
    CHECK(!range_partitions_.empty());
    while (true) {
      auto partition = range_partitions_.begin();
      std::advance(partition, rand_.Uniform(range_partitions_.size()));

      int32_t rowkey = partition->first + rand_.Uniform(partition->second - partition->first);
      if (!ContainsKey(rows_, rowkey)) {
        return rowkey;
      }
    }
  }

  string GetRandomNewColumnName() {
    while (true) {
      string name = strings::Substitute("c$0", rand_.Uniform(1000));
      if (std::find(col_names_.begin(), col_names_.end(), name) == col_names_.end()) {
        return name;
      }
    }
  }

  // Returns the name of a random column not in the primary key.
  string GetRandomExistingColumnName() {
    CHECK(col_names_.size() > 1);
    return col_names_[1 + rand_.Uniform(col_names_.size() - 1)];
  }

  int32_t GetRandomExistingRowKey() {
    CHECK(!rows_.empty());

    auto row = rows_.begin();
    std::advance(row, rand_.Uniform(rows_.size()));
    return row->first;
  }

  // Generates a random row.
  void GenRandomRow(vector<pair<string, int32_t>>* row) {
    int32_t key = GetRandomNewRowKey();

    int32_t seed = rand_.Next();
    if (seed == RowState::kNullValue || seed == RowState::kDefaultValue) {
      seed++;
    }

    row->clear();
    row->push_back(make_pair(col_names_[0], key));
    for (int i = 1; i < col_names_.size(); i++) {
      int32_t val;
      if (col_nullable_[i] && seed % 2 == 1) {
        val = RowState::kNullValue;
      } else if (seed % 3 == 0) {
        val = RowState::kDefaultValue;
      } else {
        val = seed;
      }
      row->push_back(make_pair(col_names_[i], val));
    }
  }

  bool Insert(const vector<pair<string, int32_t>>& data) {
    DCHECK_EQ(col_names_[0], data[0].first);
    int32_t key = data[0].second;
    if (ContainsKey(rows_, key)) return false;

    auto r = new RowState;
    r->cols = data;
    for (int i = 1; i < r->cols.size(); i++) {
      if (r->cols[i].second == RowState::kDefaultValue) {
        r->cols[i].second = col_defaults_[i];
      }
    }
    rows_[key].reset(r);
    return true;
  }

  bool Update(const vector<pair<string, int32_t>>& data) {
    DCHECK_EQ(col_names_[0], data[0].first);
    int32_t key = data[0].second;
    if (!ContainsKey(rows_, key)) return false;

    rows_[key]->cols = data;
    return true;
  }

  void Delete(int32_t row_key) {
    unique_ptr<RowState> r = EraseKeyReturnValuePtr(&rows_, row_key);
    CHECK(r) << "row key " << row_key << " not found";
  }

  void AddColumnWithDefault(const string& name, int32_t def, bool nullable) {
    col_names_.push_back(name);
    col_nullable_.push_back(nullable);
    col_defaults_.push_back(def);
    for (auto& e : rows_) {
      e.second->cols.push_back(make_pair(name, def));
    }
  }

  void DropColumn(const string& name) {
    auto col_it = std::find(col_names_.begin(), col_names_.end(), name);
    int index = col_it - col_names_.begin();
    col_names_.erase(col_it);
    col_nullable_.erase(col_nullable_.begin() + index);
    col_defaults_.erase(col_defaults_.begin() + index);
    for (auto& e : rows_) {
      e.second->cols.erase(e.second->cols.begin() + index);
    }
  }

  void RenameColumn(const string& existing_name, const string& new_name) {
    auto iter = std::find(col_names_.begin(), col_names_.end(), existing_name);
    CHECK(iter != col_names_.end());
    int index = iter - col_names_.begin();
    for (auto& e : rows_) {
      e.second->cols[index].first = new_name;
    }
    *iter = new_name;
  }

  void ChangeDefault(const string& name, int32_t new_def) {
    auto col_it = std::find(col_names_.begin(), col_names_.end(), name);
    CHECK(col_it != col_names_.end());
    col_defaults_[col_it - col_names_.begin()] = new_def;
  }

  pair<int32_t, int32_t> AddRangePartition() {
    CHECK(range_partitions_.size() < kMaxRangePartitions);
    while (true) {
      uint32_t width = INT32_MAX / kMaxRangePartitions;
      int32_t lower_bound = width * rand_.Uniform(kMaxRangePartitions);
      int32_t upper_bound = lower_bound + width;
      CHECK(upper_bound > lower_bound);

      if (InsertIfNotPresent(&range_partitions_, make_pair(lower_bound, upper_bound))) {
        return make_pair(lower_bound, upper_bound);
      }
    }
  }

  pair<int32_t, int32_t> DropRangePartition() {
    CHECK(!range_partitions_.empty());

    auto partition = range_partitions_.begin();
    std::advance(partition, rand_.Uniform(range_partitions_.size()));

    int32_t lower_bound = partition->first;
    int32_t upper_bound = partition->second;

    range_partitions_.erase(partition);

    rows_.erase(rows_.lower_bound(lower_bound), rows_.lower_bound(upper_bound));
    return make_pair(lower_bound, upper_bound);
  }

  void ToStrings(vector<string>* strs) {
    strs->clear();
    for (const auto& e : rows_) {
      strs->push_back(e.second->ToString());
    }
  }

  // The name of each column.
  vector<string> col_names_;

  // For each column, whether it is NULLable.
  // Has the same length as col_names_.
  vector<bool> col_nullable_;

  // For each column, its current write default.
  // Has the same length as col_names_.
  vector<int32_t> col_defaults_;

  map<int32_t, unique_ptr<RowState>> rows_;

  // The lower and upper bounds of all range partitions in the table.
  map<int32_t, int32_t> range_partitions_;

  Random rand_;
};

struct MirrorTable {
  explicit MirrorTable(shared_ptr<KuduClient> client)
      : client_(std::move(client)) {}

  Status Create() {
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    CHECK_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    table_creator->table_name(kTableName)
                  .schema(&schema)
                  .set_range_partition_columns({ "key" })
                  .num_replicas(3);

    for (const auto& partition : ts_.range_partitions_) {
      unique_ptr<KuduPartialRow> lower(schema.NewRow());
      unique_ptr<KuduPartialRow> upper(schema.NewRow());
      RETURN_NOT_OK(lower->SetInt32("key", partition.first));
      RETURN_NOT_OK(upper->SetInt32("key", partition.second));
      table_creator->add_range_partition(lower.release(), upper.release());
    }

    return table_creator->Create();
  }

  void InsertRandomRow() {
    vector<pair<string, int32_t>> row;
    ts_.GenRandomRow(&row);
    Status s = DoRealOp(row, INSERT);
    if (s.IsAlreadyPresent()) {
      CHECK(!ts_.Insert(row)) << "real table said already-present, fake table succeeded";
    }
    CHECK_OK(s);

    CHECK(ts_.Insert(row));
  }

  void DeleteRandomRow() {
    if (ts_.rows_.empty()) return;
    int32_t row_key = ts_.GetRandomExistingRowKey();
    vector<pair<string, int32_t>> del;
    del.push_back(make_pair(ts_.col_names_[0], row_key));
    CHECK_OK(DoRealOp(del, DELETE));

    ts_.Delete(row_key);
  }

  void UpdateRandomRow(uint32_t rand) {
    if (ts_.rows_.empty()) return;
    int32_t row_key = ts_.GetRandomExistingRowKey();

    vector<pair<string, int32_t>> update;
    update.push_back(make_pair(ts_.col_names_[0], row_key));
    for (int i = 1; i < num_columns(); i++) {
      int32_t val = rand * i;
      if (val == RowState::kNullValue) val++;
      if (val == RowState::kDefaultValue) val++;
      if (ts_.col_nullable_[i] && val % 2 == 1) {
        val = RowState::kNullValue;
      }
      update.push_back(make_pair(ts_.col_names_[i], val));
    }

    if (update.size() == 1) {
      // No columns got updated. Just ignore this update.
      return;
    }

    Status s = DoRealOp(update, UPDATE);
    if (s.IsNotFound()) {
      CHECK(!ts_.Update(update)) << "real table said not-found, fake table succeeded";
      return;
    }
    CHECK_OK(s);

    CHECK(ts_.Update(update));
  }

  void RandomAlterTable() {
    LOG(INFO) << "Beginning Alterations";

    // This schema must outlive the table alterer, since the rows in add/drop
    // range partition hold a pointer to it.
    KuduSchema schema;
    CHECK_OK(client_->GetTableSchema(kTableName, &schema));
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));

    int step_count = 1 + ts_.rand_.Uniform(10);
    for (int step = 0; step < step_count; step++) {
      int r = ts_.rand_.Uniform(9);
      if (r < 1 && num_columns() < kMaxColumns) {
        AddAColumn(table_alterer.get());
      } else if (r < 2 && num_columns() > 1) {
        DropAColumn(table_alterer.get());
      } else if (num_range_partitions() == 0 ||
                 (r < 3 && num_range_partitions() < kMaxRangePartitions)) {
        AddARangePartition(schema, table_alterer.get());
      } else if (r < 4 && num_columns() > 1) {
        RenameAColumn(table_alterer.get());
      } else if (r < 5 && num_columns() > 1) {
        RenamePrimaryKeyColumn(table_alterer.get());
      } else if (r < 6 && num_columns() > 1) {
        RenameAColumn(table_alterer.get());
      } else if (r < 7 && num_columns() > 1) {
        ChangeADefault(table_alterer.get());
      } else if (r < 8 && num_columns() > 1) {
        ChangeAStorageAttribute(table_alterer.get());
      } else {
        DropARangePartition(schema, table_alterer.get());
      }
    }
    LOG(INFO) << "Committing Alterations";
    CHECK_OK(table_alterer->Alter());
  }

  void AddAColumn(KuduTableAlterer* table_alterer) {
    string name = ts_.GetRandomNewColumnName();
    LOG(INFO) << "Adding column " << name << ", existing columns: "
              << JoinStrings(ts_.col_names_, ", ");

    int32_t default_value = rand();
    bool nullable = rand() % 2 == 1;

    // Add to the real table.
    if (nullable) {
      default_value = RowState::kNullValue;
      table_alterer->AddColumn(name)->Type(KuduColumnSchema::INT32);
    } else {
      table_alterer->AddColumn(name)->Type(KuduColumnSchema::INT32)->NotNull()
                   ->Default(KuduValue::FromInt(default_value));
    }

    // Add to the mirror state.
    ts_.AddColumnWithDefault(name, default_value, nullable);
  }

  void DropAColumn(KuduTableAlterer* table_alterer) {
    string name = ts_.GetRandomExistingColumnName();
    LOG(INFO) << "Dropping column " << name << ", existing columns: "
              << JoinStrings(ts_.col_names_, ", ");
    table_alterer->DropColumn(name);
    ts_.DropColumn(name);
  }

  void RenameAColumn(KuduTableAlterer* table_alterer) {
    string original_name = ts_.GetRandomExistingColumnName();
    string new_name = ts_.GetRandomNewColumnName();
    LOG(INFO) << "Renaming column " << original_name << " to " << new_name;
    table_alterer->AlterColumn(original_name)->RenameTo(new_name);
    ts_.RenameColumn(original_name, new_name);
  }

  void RenamePrimaryKeyColumn(KuduTableAlterer* table_alterer) {
    string new_name = ts_.GetRandomNewColumnName();
    LOG(INFO) << "Renaming PrimaryKey column " << ts_.col_names_[0] << " to " << new_name;
    table_alterer->AlterColumn(ts_.col_names_[0])->RenameTo(new_name);
    ts_.RenameColumn(ts_.col_names_[0], new_name);
  }

  void ChangeADefault(KuduTableAlterer* table_alterer) {
    string name = ts_.GetRandomExistingColumnName();
    int32_t new_def = ts_.rand_.Next();
    if (new_def == RowState::kNullValue || new_def == RowState::kDefaultValue) {
      new_def++;
    }
    LOG(INFO) << "Changing default of column " << name << " to " << new_def;
    table_alterer->AlterColumn(name)->Default(KuduValue::FromInt(new_def));
    ts_.ChangeDefault(name, new_def);
  }

  void ChangeAStorageAttribute(KuduTableAlterer* table_alterer) {
    string name = ts_.GetRandomExistingColumnName();
    int type = ts_.rand_.Uniform(3);
    if (type == 0) {
      int i = ts_.rand_.Uniform(kCompressionTypes.size());
      LOG(INFO) << "Changing compression of column " << name <<
                " to " << kCompressionTypes[i];
      table_alterer->AlterColumn(name)->Compression(kCompressionTypes[i]);
    } else if (type == 1) {
      int i = ts_.rand_.Uniform(kInt32Encodings.size());
      LOG(INFO) << "Changing encoding of column " << name <<
                " to " << kInt32Encodings[i];
      table_alterer->AlterColumn(name)->Encoding(kInt32Encodings[i]);
    } else {
      int i = ts_.rand_.Uniform(kBlockSizes.size());
      LOG(INFO) << "Changing block size of column " << name <<
                " to " << kBlockSizes[i];
      table_alterer->AlterColumn(name)->BlockSize(kBlockSizes[i]);
    }
  }

  void AddARangePartition(const KuduSchema& schema, KuduTableAlterer* table_alterer) {
    auto bounds = ts_.AddRangePartition();
    LOG(INFO) << "Adding range partition: [" << bounds.first << ", " << bounds.second << ")"
              << " resulting partitions: ("
              << JoinKeysAndValuesIterator(ts_.range_partitions_.begin(),
                                           ts_.range_partitions_.end(),
                                           ", ", "], (") << ")";

    KuduTableCreator::RangePartitionBound lower_bound_type = KuduTableCreator::INCLUSIVE_BOUND;
    KuduTableCreator::RangePartitionBound upper_bound_type = KuduTableCreator::EXCLUSIVE_BOUND;
    int32_t lower_bound_value = bounds.first;
    int32_t upper_bound_value = bounds.second;

    if (ts_.rand_.OneIn(2) && lower_bound_value > INT32_MIN) {
      lower_bound_type = KuduTableCreator::EXCLUSIVE_BOUND;
      lower_bound_value -= 1;
    }

    if (ts_.rand_.OneIn(2)) {
      upper_bound_type = KuduTableCreator::INCLUSIVE_BOUND;
      upper_bound_value -= 1;
    }

    unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
    CHECK_OK(lower_bound->SetInt32(schema.Column(0).name(), lower_bound_value));
    unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
    CHECK_OK(upper_bound->SetInt32(schema.Column(0).name(), upper_bound_value));

    table_alterer->AddRangePartition(lower_bound.release(), upper_bound.release(),
                                     lower_bound_type, upper_bound_type);
  }

  void DropARangePartition(KuduSchema& schema, KuduTableAlterer* table_alterer) {
    auto bounds = ts_.DropRangePartition();
    LOG(INFO) << "Dropping range partition: [" << bounds.first << ", " << bounds.second << ")"
              << " resulting partitions: ("
              << JoinKeysAndValuesIterator(ts_.range_partitions_.begin(),
                                           ts_.range_partitions_.end(),
                                           ", ", "], (") << ")";

    unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
    CHECK_OK(lower_bound->SetInt32(schema.Column(0).name(), bounds.first));
    unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
    CHECK_OK(upper_bound->SetInt32(schema.Column(0).name(), bounds.second));

    table_alterer->DropRangePartition(lower_bound.release(), upper_bound.release());
  }

  int num_columns() const {
    return ts_.col_names_.size();
  }

  int num_rows() const {
    return ts_.rows_.size();
  }

  int num_range_partitions() const {
    return ts_.range_partitions_.size();
  }

  void Verify() {
    LOG(INFO) << "Verifying " << ts_.rows_.size() << " rows in "
              << ts_.range_partitions_.size() << " tablets";
    // First scan the real table
    vector<string> rows;
    {
      shared_ptr<KuduTable> table;
      CHECK_OK(client_->OpenTable(kTableName, &table));
      KuduScanner scanner(table.get());
      ASSERT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
      ASSERT_OK(scanner.SetFaultTolerant());
      scanner.SetTimeoutMillis(60000);
      NO_FATALS(ScanToStrings(&scanner, &rows));
    }

    // Then get our mock table.
    vector<string> expected;
    ts_.ToStrings(&expected);

    // They should look the same.
    ASSERT_EQ(expected, rows);
  }

 private:
  enum OpType {
    INSERT, UPDATE, DELETE
  };

  Status DoRealOp(const vector<pair<string, int32_t>>& data, OpType op_type) {
    VLOG(1) << "Applying op " << op_type << " " << data[0].first << ": " << data[0].second;
    shared_ptr<KuduSession> session = client_->NewSession();
    shared_ptr<KuduTable> table;
    RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(60 * 1000);
    RETURN_NOT_OK(client_->OpenTable(kTableName, &table));
    unique_ptr<KuduWriteOperation> op;
    switch (op_type) {
      case INSERT: op.reset(table->NewInsert()); break;
      case UPDATE: op.reset(table->NewUpdate()); break;
      case DELETE: op.reset(table->NewDelete()); break;
    }
    for (int i = 0; i < data.size(); i++) {
      const auto& d = data[i];
      if (d.second == RowState::kNullValue) {
        CHECK_OK(op->mutable_row()->SetNull(d.first));
      } else if (d.second == RowState::kDefaultValue) {
        if (ts_.col_defaults_[i] == RowState::kNullValue) {
          CHECK_OK(op->mutable_row()->SetNull(d.first));
        } else {
          CHECK_OK(op->mutable_row()->SetInt32(d.first, ts_.col_defaults_[i]));
        }
      } else {
        CHECK_OK(op->mutable_row()->SetInt32(d.first, d.second));
      }
    }
    RETURN_NOT_OK(session->Apply(op.release()));
    Status s = session->Flush();
    if (s.ok()) {
      return s;
    }

    std::vector<KuduError*> errors;
    ElementDeleter d(&errors);
    bool overflow;
    session->GetPendingErrors(&errors, &overflow);
    CHECK_EQ(errors.size(), 1);
    return errors[0]->status();
  }

  shared_ptr<KuduClient> client_;
  TableState ts_;
};

// Stress test for various alter table scenarios. This performs a random sequence of:
//   - insert a row (using the latest schema)
//   - delete a random row
//   - update a row (all columns with the latest schema)
//   - add a new column
//   - drop a column
//   - restart the tablet server
//
// During the sequence of operations, a "mirror" of the table in memory is kept up to
// date. We periodically scan the actual table, and ensure that the data in Kudu
// matches our in-memory "mirror".
TEST_F(AlterTableRandomized, TestRandomSequence) {
  MirrorTable t(client_);
  ASSERT_OK(t.Create());

  Random rng(SeedRandom());

  const int n_iters = AllowSlowTests() ? 2000 : 1000;
  for (int i = 0; i < n_iters; i++) {
    // Perform different operations with varying probability.
    // We mostly insert and update, with occasional deletes,
    // and more occasional table alterations or restarts.

    int r = rng.Uniform(1000);

    if (r < 3) {
      RestartMaster();
    } else if (r < 10) {
      RestartTabletServer(rng.Uniform(cluster_->num_tablet_servers()));
    } else if (r < 35 || t.num_range_partitions() == 0) {
      t.RandomAlterTable();
    } else if (r < 500) {
      t.InsertRandomRow();
    } else if (r < 750) {
      t.DeleteRandomRow();
    } else {
      t.UpdateRandomRow(rng.Next());
    }

    if (i % 1000 == 0) {
      NO_FATALS(t.Verify());
    }
  }

  NO_FATALS(t.Verify());

  // Not only should the data returned by a scanner match what we expect,
  // we also expect all of the replicas to agree with each other.
  LOG(INFO) << "Verifying cluster";
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
}

} // namespace kudu
