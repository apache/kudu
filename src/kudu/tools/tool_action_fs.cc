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

#include "kudu/tools/tool_action.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/container/flat_map.hpp>
#include <boost/container/vector.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/type_encodings.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/fs_report.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/ascii_ctype.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DECLARE_bool(force);
DECLARE_bool(print_meta);
DECLARE_string(columns);

DEFINE_bool(print_rows, true,
            "Print each row in the CFile");
DEFINE_string(uuid, "",
              "The uuid to use in the filesystem. "
              "If not provided, one is generated");
DEFINE_bool(repair, false,
            "Repair any inconsistencies in the filesystem.");

DEFINE_string(table_id, "",
              "Restrict output to a specific table by id");
DECLARE_string(table_name);
DEFINE_string(tablet_id, "",
              "Restrict output to a specific tablet");
DEFINE_int64(rowset_id, -1,
             "Restrict output to a specific rowset");
DEFINE_int32(column_id, -1,
             "Restrict output to a specific column");
DEFINE_uint64(block_id, 0,
              "Restrict output to a specific block");
DEFINE_bool(h, true,
            "Pretty-print values in human-readable units");

namespace kudu {
namespace tools {

using cfile::CFileIterator;
using cfile::CFileReader;
using cfile::ReaderOptions;
using fs::BlockDeletionTransaction;
using fs::ConsistencyCheckBehavior;
using fs::FsReport;
using fs::ReadableBlock;
using std::cout;
using std::endl;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;
using tablet::RowSetMetadata;
using tablet::TabletMetadata;

namespace {

Status Check(const RunnerContext& /*context*/) {
  FsManagerOpts fs_opts;
  fs_opts.read_only = !FLAGS_repair;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  FsReport report;
  RETURN_NOT_OK(fs_manager.Open(&report));

  // Stop now if we've already found a fatal error. Otherwise, continue;
  // we'll modify the report with our own check results and print it fully
  // at the end.
  if (report.HasFatalErrors()) {
    RETURN_NOT_OK(report.PrintAndCheckForFatalErrors());
  }

  // Get the "live" block IDs (i.e. those referenced by a tablet).
  vector<BlockId> live_block_ids;
  unordered_map<BlockId, string, BlockIdHash, BlockIdEqual> live_block_id_to_tablet;
  vector<string> tablet_ids;
  RETURN_NOT_OK(fs_manager.ListTabletIds(&tablet_ids));
  for (const auto& t : tablet_ids) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK(TabletMetadata::Load(&fs_manager, t, &meta));
    vector<BlockId> tablet_live_block_ids = meta->CollectBlockIds();
    live_block_ids.insert(live_block_ids.end(),
                          tablet_live_block_ids.begin(),
                          tablet_live_block_ids.end());
    for (const auto& id : tablet_live_block_ids) {
      InsertOrDie(&live_block_id_to_tablet, id, t);
    }
  }

  // Get all of the block IDs reachable by the block manager.
  vector<BlockId> all_block_ids;
  RETURN_NOT_OK(fs_manager.block_manager()->GetAllBlockIds(&all_block_ids));

  std::sort(live_block_ids.begin(), live_block_ids.end(), BlockIdCompare());
  std::sort(all_block_ids.begin(), all_block_ids.end(), BlockIdCompare());

  // Blocks found in the block manager but not in a tablet. They are orphaned
  // and can be safely deleted.
  vector<BlockId> orphaned_block_ids;
  std::set_difference(all_block_ids.begin(), all_block_ids.end(),
                      live_block_ids.begin(), live_block_ids.end(),
                      std::back_inserter(orphaned_block_ids), BlockIdCompare());

  // Blocks found in a tablet but not in the block manager. They are missing
  // and indicative of corruption in the associated tablet(s).
  vector<BlockId> missing_block_ids;
  std::set_difference(live_block_ids.begin(), live_block_ids.end(),
                      all_block_ids.begin(), all_block_ids.end(),
                      std::back_inserter(missing_block_ids), BlockIdCompare());

  // Add missing blocks to the report.
  report.missing_block_check.emplace();
  for (const auto& id : missing_block_ids) {
    report.missing_block_check->entries.emplace_back(
        id, FindOrDie(live_block_id_to_tablet, id));
  }

  // Add orphaned blocks to the report after attempting to repair them.
  report.orphaned_block_check.emplace();
  shared_ptr<BlockDeletionTransaction> deletion_transaction;
  if (FLAGS_repair) {
    deletion_transaction = fs_manager.block_manager()->NewDeletionTransaction();
  }
  vector<BlockId> deleted;
  for (const auto& id : orphaned_block_ids) {
    // Opening a block isn't free, but the number of orphaned blocks shouldn't
    // be extraordinarily high.
    uint64_t size;
    {
      unique_ptr<ReadableBlock> block;
      RETURN_NOT_OK(fs_manager.OpenBlock(id, &block));
      RETURN_NOT_OK(block->Size(&size));
    }
    fs::OrphanedBlockCheck::Entry entry(id, size);

    if (FLAGS_repair) {
      deletion_transaction->AddDeletedBlock(id);
    }
    report.orphaned_block_check->entries.emplace_back(entry);
  }

  if (FLAGS_repair) {
    WARN_NOT_OK(deletion_transaction->CommitDeletedBlocks(&deleted),
                "Could not delete orphaned blocks");
    BlockIdSet deleted_set(deleted.begin(), deleted.end());
    for (auto& entry : report.orphaned_block_check->entries) {
      if (ContainsKey(deleted_set, entry.block_id)) entry.repaired = true;
    }
  }

  return report.PrintAndCheckForFatalErrors();
}

Status Format(const RunnerContext& /*context*/) {
  FsManager fs_manager(Env::Default(), FsManagerOpts());
  boost::optional<string> uuid;
  if (!FLAGS_uuid.empty()) {
    uuid = FLAGS_uuid;
  }
  return fs_manager.CreateInitialFileSystemLayout(uuid);
}

Status DumpUuid(const RunnerContext& /*context*/) {
  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.Open());
  cout << fs_manager.uuid() << endl;
  return Status::OK();
}

Status ParseBlockIdArg(const RunnerContext& context,
                       BlockId* id) {
  const string& block_id_str = FindOrDie(context.required_args, "block_id");
  uint64_t numeric_id;
  if (!safe_strtou64(block_id_str, &numeric_id)) {
    return Status::InvalidArgument(Substitute(
        "Could not parse $0 as numeric block ID", block_id_str));
  }
  *id = BlockId(numeric_id);
  return Status::OK();
}

Status DumpCFile(const RunnerContext& context) {
  BlockId block_id;
  RETURN_NOT_OK(ParseBlockIdArg(context, &block_id));

  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.Open());

  unique_ptr<fs::ReadableBlock> block;
  RETURN_NOT_OK(fs_manager.OpenBlock(block_id, &block));

  unique_ptr<CFileReader> reader;
  RETURN_NOT_OK(CFileReader::Open(std::move(block), ReaderOptions(), &reader));

  if (FLAGS_print_meta) {
    cout << "Header:\n" << pb_util::SecureDebugString(reader->header()) << endl;
    cout << "Footer:\n" << pb_util::SecureDebugString(reader->footer()) << endl;
  }

  if (FLAGS_print_rows) {
    gscoped_ptr<CFileIterator> it;
    RETURN_NOT_OK(reader->NewIterator(&it, CFileReader::DONT_CACHE_BLOCK, nullptr));
    RETURN_NOT_OK(it->SeekToFirst());

    RETURN_NOT_OK(DumpIterator(*reader, it.get(), &cout, 0, 0));
  }

  return Status::OK();
}

Status DumpBlock(const RunnerContext& context) {
  BlockId block_id;
  RETURN_NOT_OK(ParseBlockIdArg(context, &block_id));

  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.Open());

  unique_ptr<fs::ReadableBlock> block;
  RETURN_NOT_OK(fs_manager.OpenBlock(block_id, &block));

  uint64_t size = 0;
  RETURN_NOT_OK_PREPEND(block->Size(&size), "couldn't get block size");

  faststring buf;
  uint64_t offset = 0;
  while (offset < size) {
    int64_t chunk = std::min<int64_t>(size - offset, 64 * 1024);
    buf.resize(chunk);
    Slice s(buf);
    RETURN_NOT_OK(block->Read(offset, s));
    offset += s.size();
    cout << s.ToString();
  }

  return Status::OK();
}

Status DumpFsTree(const RunnerContext& /*context*/) {
  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.Open());

  fs_manager.DumpFileSystemTree(std::cout);
  return Status::OK();
}

Status CheckForTabletsThatWillFailWithUpdate() {
  FsManagerOpts opts;
  opts.read_only = true;
  opts.consistency_check = ConsistencyCheckBehavior::IGNORE_INCONSISTENCY;
  FsManager fs(Env::Default(), std::move(opts));
  RETURN_NOT_OK(fs.Open());

  vector<string> tablet_ids;
  RETURN_NOT_OK(fs.ListTabletIds(&tablet_ids));
  for (const auto& t : tablet_ids) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK(TabletMetadata::Load(&fs, t, &meta));
    DataDirGroupPB group;
    RETURN_NOT_OK_PREPEND(
        fs.dd_manager()->GetDataDirGroupPB(t, &group),
        "at least one tablet is configured to use removed data directory. "
        "Retry with --force to override this");
  }
  return Status::OK();
}

Status Update(const RunnerContext& /*context*/) {
  Env* env = Env::Default();

  // First, ensure that if we're removing a data directory, no existing tablets
  // are configured to use it. We approximate this by creating an FsManager
  // that reflects the new data directory configuration, loading all tablet
  // metadata, and retrieving all tablets' data dir groups. If a data dir group
  // cannot be retrieved, it is assumed that it's because the tablet is
  // configured to use a data directory that's now missing.
  //
  // If the user specifies --force, we assume they know what they're doing and
  // skip this check.
  Status s = CheckForTabletsThatWillFailWithUpdate();
  if (FLAGS_force) {
    WARN_NOT_OK(s, "continuing due to --force");
  } else {
    RETURN_NOT_OK_PREPEND(s, "cannot update data directories");
  }

  // Now perform the update.
  FsManagerOpts opts;
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  FsManager fs(env, std::move(opts));
  return fs.Open();
}

namespace {

// The 'kudu fs list' column fields.
//
// Field is synonymous with a data-table column, but internally we use 'field'
// in order to disambiguate with Kudu columns.
enum class Field {

  // Tablet-specific information:
  kTable,
  kTableId,
  kTabletId,
  kPartition,

  // Rowset-specific information:
  kRowsetId,

  // Block-specific information:
  kBlockId,
  kBlockKind,
  kColumn,
  kColumnId,

  // CFile specific information:
  kCFileDataType,
  kCFileNullable,
  kCFileEncoding,
  kCFileCompression,
  kCFileNumValues,
  kCFileSize,
  kCFileMinKey,
  kCFileMaxKey,
  kCFileIncompatibleFeatures,
  kCFileCompatibleFeatures,
  kCFileDeltaStats,
};

// Enumerable array of field variants. Must be kept in-sync with the Field enum class.
const Field kFieldVariants[] = {
  Field::kTable,
  Field::kTableId,
  Field::kTabletId,
  Field::kPartition,
  Field::kRowsetId,
  Field::kBlockId,
  Field::kBlockKind,
  Field::kColumn,
  Field::kColumnId,
  Field::kCFileDataType,
  Field::kCFileNullable,
  Field::kCFileEncoding,
  Field::kCFileCompression,
  Field::kCFileNumValues,
  Field::kCFileSize,
  Field::kCFileIncompatibleFeatures,
  Field::kCFileCompatibleFeatures,
  Field::kCFileMinKey,
  Field::kCFileMaxKey,
  Field::kCFileDeltaStats,
};

// Groups the fields into categories based on their cardinality and required metadata.
enum class FieldGroup {
  // Cardinality: 1 row per tablet
  // Metadata: TabletMetadata
  kTablet,

  // Cardinality: 1 row per rowset per tablet
  // Metadata: RowSetMetadata, TabletMetadata
  kRowset,

  // Cardinality: 1 row per block per rowset per tablet
  // Metadata: RowSetMetadata, TabletMetadata
  kBlock,

  // Cardinality: 1 row per block per rowset per tablet
  // Metadata: CFileReader, RowSetMetadata, TabletMetadata
  kCFile,
};

// Returns the pretty-printed field name.
const char* ToString(Field field) {
  switch (field) {
    case Field::kTable: return "table";
    case Field::kTableId: return "table-id";
    case Field::kTabletId: return "tablet-id";
    case Field::kPartition: return "partition";
    case Field::kRowsetId: return "rowset-id";
    case Field::kBlockId: return "block-id";
    case Field::kBlockKind: return "block-kind";
    case Field::kColumn: return "column";
    case Field::kColumnId: return "column-id";
    case Field::kCFileDataType: return "cfile-data-type";
    case Field::kCFileNullable: return "cfile-nullable";
    case Field::kCFileEncoding: return "cfile-encoding";
    case Field::kCFileCompression: return "cfile-compression";
    case Field::kCFileNumValues: return "cfile-num-values";
    case Field::kCFileSize: return "cfile-size";
    case Field::kCFileIncompatibleFeatures: return "cfile-incompatible-features";
    case Field::kCFileCompatibleFeatures: return "cfile-compatible-features";
    case Field::kCFileMinKey: return "cfile-min-key";
    case Field::kCFileMaxKey: return "cfile-max-key";
    case Field::kCFileDeltaStats: return "cfile-delta-stats";
  }
  LOG(FATAL) << "unhandled field (this is a bug)";
}

// Returns the pretty-printed group name.
const char* ToString(FieldGroup group) {
  switch (group) {
    case FieldGroup::kTablet: return "tablet";
    case FieldGroup::kRowset: return "rowset";
    case FieldGroup::kBlock: return "block";
    case FieldGroup::kCFile: return "cfile";
    default: LOG(FATAL) << "unhandled field group (this is a bug)";
  }
}

// Transforms an ASCII string to lowercase.
void ToLowerCase(string* string) {
  std::transform(string->begin(), string->end(), string->begin(), ascii_tolower);
}

// Parses a field name and returns the corresponding enum variant.
Status ParseField(string name, Field* field) {
  StripWhiteSpace(&name);
  StripString(&name, "_", '-');
  ToLowerCase(&name);

  for (Field variant : kFieldVariants) {
    if (name == ToString(variant)) {
      *field = variant;
      return Status::OK();
    }
  }

  return Status::InvalidArgument("unknown column", name);
}

FieldGroup ToFieldGroup(Field field) {
  switch (field) {
    case Field::kTable:
    case Field::kTableId:
    case Field::kTabletId:
    case Field::kPartition: return FieldGroup::kTablet;

    case Field::kRowsetId: return FieldGroup::kRowset;

    case Field::kBlockId:
    case Field::kBlockKind:
    case Field::kColumn:
    case Field::kColumnId: return FieldGroup::kBlock;

    case Field::kCFileDataType:
    case Field::kCFileNullable:
    case Field::kCFileEncoding:
    case Field::kCFileCompression:
    case Field::kCFileNumValues:
    case Field::kCFileSize:
    case Field::kCFileIncompatibleFeatures:
    case Field::kCFileCompatibleFeatures:
    case Field::kCFileMinKey:
    case Field::kCFileMaxKey:
    case Field::kCFileDeltaStats: return FieldGroup::kCFile;
  }
  LOG(FATAL) << "unhandled field (this is a bug): " << ToString(field);
}

// Returns tablet info for the field.
string TabletInfo(Field field, const TabletMetadata& tablet) {
  switch (field) {
    case Field::kTable: return tablet.table_name();
    case Field::kTableId: return tablet.table_id();
    case Field::kTabletId: return tablet.tablet_id();
    case Field::kPartition: return tablet.partition_schema()
                                         .PartitionDebugString(tablet.partition(),
                                                               tablet.schema());
    default: LOG(FATAL) << "unhandled field (this is a bug): " << ToString(field);
  }
}

// Returns rowset info for the field.
string RowsetInfo(Field field, const TabletMetadata& tablet, const RowSetMetadata& rowset) {
  switch (field) {
    case Field::kRowsetId: return std::to_string(rowset.id());
    default: return TabletInfo(field, tablet);
  }
}

// Returns block info for the field.
string BlockInfo(Field field,
                 const TabletMetadata& tablet,
                 const RowSetMetadata& rowset,
                 const char* block_kind,
                 boost::optional<ColumnId> column_id,
                 const BlockId& block) {
  CHECK(!block.IsNull());
  switch (field) {
    case Field::kBlockId: return std::to_string(block.id());
    case Field::kBlockKind: return block_kind;

    case Field::kColumn: if (column_id) {
      return tablet.schema().column_by_id(*column_id).name();
    } else { return ""; }

    case Field::kColumnId: if (column_id) {
      return std::to_string(column_id.get());
    } else { return ""; }

    default: return RowsetInfo(field, tablet, rowset);
  }
}

// Formats the min or max primary key property from CFile metadata.
string FormatCFileKeyMetadata(const TabletMetadata& tablet,
                              const CFileReader& cfile,
                              const char* property) {
  string value;
  if (!cfile.GetMetadataEntry(property, &value)) {
    return "";
  }

  Arena arena(1024);
  gscoped_ptr<EncodedKey> key;
  CHECK_OK(EncodedKey::DecodeEncodedString(tablet.schema(), &arena, value, &key));
  return key->Stringify(tablet.schema());
}

// Formats the delta stats property from CFile metadata.
string FormatCFileDeltaStats(const CFileReader& cfile) {
  string value;
  if (!cfile.GetMetadataEntry(tablet::DeltaFileReader::kDeltaStatsEntryName, &value)) {
    return "";
  }

  tablet::DeltaStatsPB deltastats_pb;
  CHECK(deltastats_pb.ParseFromString(value))
      << "failed to decode delta stats for block " << cfile.block_id();

  tablet::DeltaStats deltastats;
  CHECK_OK(deltastats.InitFromPB(deltastats_pb));
  return deltastats.ToString();
}

// Returns cfile info for the field.
string CFileInfo(Field field,
                 const TabletMetadata& tablet,
                 const RowSetMetadata& rowset,
                 const char* block_kind,
                 const boost::optional<ColumnId>& column_id,
                 const BlockId& block,
                 const CFileReader& cfile) {
  switch (field) {
    case Field::kCFileDataType:
      return cfile.type_info()->name();
    case Field::kCFileNullable:
      return cfile.is_nullable() ? "true" : "false";
    case Field::kCFileEncoding:
      return EncodingType_Name(cfile.type_encoding_info()->encoding_type());
    case Field::kCFileCompression:
      return CompressionType_Name(cfile.footer().compression());
    case Field::kCFileNumValues: if (FLAGS_h) {
      return HumanReadableNum::ToString(cfile.footer().num_values());
    } else {
      return std::to_string(cfile.footer().num_values());
    }
    case Field::kCFileSize: if (FLAGS_h) {
      return HumanReadableNumBytes::ToString(cfile.file_size());
    } else {
      return std::to_string(cfile.file_size());
    }
    case Field::kCFileIncompatibleFeatures:
      return std::to_string(cfile.footer().incompatible_features());
    case Field::kCFileCompatibleFeatures:
      return std::to_string(cfile.footer().compatible_features());
    case Field::kCFileMinKey:
      return FormatCFileKeyMetadata(tablet, cfile, tablet::DiskRowSet::kMinKeyMetaEntryName);
    case Field::kCFileMaxKey:
      return FormatCFileKeyMetadata(tablet, cfile, tablet::DiskRowSet::kMaxKeyMetaEntryName);
    case Field::kCFileDeltaStats:
      return FormatCFileDeltaStats(cfile);
    default: return BlockInfo(field, tablet, rowset, block_kind, column_id, block);
  }
}

// Helper function that calls one of the above info functions repeatedly to
// build up a row.
template<typename F, typename... Params>
vector<string> BuildInfoRow(F info_func,
                            const vector<Field>& fields,
                            const Params&... params) {
  vector<string> row;
  row.reserve(fields.size());
  for (Field field : fields) {
    row.emplace_back(info_func(field, params...));
  }
  return row;
}

// Helper function that opens a CFile, if necessary, builds up a row, and adds
// it to the data table.
//
// If the block ID isn't valid or doesn't match the block ID filter, then the
// block is skipped.
Status AddBlockInfoRow(DataTable* table,
                       FieldGroup group,
                       const vector<Field>& fields,
                       FsManager* fs_manager,
                       const TabletMetadata& tablet,
                       const RowSetMetadata& rowset,
                       const char* block_kind,
                       const boost::optional<ColumnId>& column_id,
                       const BlockId& block) {
  if (block.IsNull() || (FLAGS_block_id > 0 && FLAGS_block_id != block.id())) {
    return Status::OK();
  }
  if (group == FieldGroup::kCFile) {
    unique_ptr<CFileReader> cfile;
    unique_ptr<ReadableBlock> readable_block;
    RETURN_NOT_OK(fs_manager->OpenBlock(block, &readable_block));
    RETURN_NOT_OK(CFileReader::Open(std::move(readable_block), ReaderOptions(), &cfile));
    table->AddRow(BuildInfoRow(CFileInfo, fields, tablet, rowset, block_kind,
                               column_id, block, *cfile));

  } else {
    table->AddRow(BuildInfoRow(BlockInfo, fields, tablet, rowset, block_kind,
                               column_id, block));
  }
  return Status::OK();
}
} // anonymous namespace

Status List(const RunnerContext& /*context*/) {
  // Parse the required fields into the enum form, and create an output data table.
  vector<Field> fields;
  vector<string> columns;
  for (StringPiece name : strings::Split(FLAGS_columns, ",", strings::SkipEmpty())) {
    Field field;
    RETURN_NOT_OK(ParseField(name.ToString(), &field));
    fields.push_back(field);
    columns.emplace_back(ToString(field));
  }
  DataTable table(std::move(columns));

  if (fields.empty()) {
    return table.PrintTo(cout);
  }

  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.Open());

  // Build the list of tablets to inspect.
  vector<string> tablet_ids;
  if (!FLAGS_tablet_id.empty()) {
    string tablet_id = FLAGS_tablet_id;
    ToLowerCase(&tablet_id);
    tablet_ids.emplace_back(std::move(tablet_id));
  } else {
    RETURN_NOT_OK(fs_manager.ListTabletIds(&tablet_ids));
  }

  string table_name = FLAGS_table_name;
  string table_id = FLAGS_table_id;
  ToLowerCase(&table_id);

  FieldGroup group = ToFieldGroup(*std::max_element(fields.begin(), fields.end()));
  VLOG(1) << "group: " << string(ToString(group));

  for (const string& tablet_id : tablet_ids) {
    scoped_refptr<TabletMetadata> tablet_metadata;
    RETURN_NOT_OK(TabletMetadata::Load(&fs_manager, tablet_id, &tablet_metadata));
    const TabletMetadata& tablet = *tablet_metadata.get();

    if (!table_name.empty() && table_name != tablet.table_name()) {
      continue;
    }

    if (!table_id.empty() && table_id != tablet.table_id()) {
      continue;
    }

    if (group == FieldGroup::kTablet) {
      table.AddRow(BuildInfoRow(TabletInfo, fields, tablet));
      continue;
    }

    for (const auto& rowset_metadata : tablet.rowsets()) {
      const RowSetMetadata& rowset = *rowset_metadata.get();

      if (FLAGS_rowset_id != -1 && FLAGS_rowset_id != rowset.id()) {
        continue;
      }

      if (group == FieldGroup::kRowset) {
        table.AddRow(BuildInfoRow(RowsetInfo, fields, tablet, rowset));
        continue;
      }

      auto column_blocks = rowset.GetColumnBlocksById();
      if (FLAGS_column_id >= 0) {
        ColumnId column_id(FLAGS_column_id);
        auto block = FindOrNull(column_blocks, column_id);
        if (block) {
          RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet, rowset,
                                        "column", column_id, *block));
        }
      } else {
        for (const auto& col_block : column_blocks) {
          RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet,
                                        rowset, "column", col_block.first, col_block.second));
        }
        for (const auto& block : rowset.redo_delta_blocks()) {
          RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet,
                                        rowset, "redo", boost::none, block));
        }
        for (const auto& block : rowset.undo_delta_blocks()) {
          RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet,
                                        rowset, "undo", boost::none, block));
        }
        RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet,
                                      rowset, "bloom", boost::none, rowset.bloom_block()));
        RETURN_NOT_OK(AddBlockInfoRow(&table, group, fields, &fs_manager, tablet,
                                      rowset, "adhoc-index", boost::none,
                                      rowset.adhoc_index_block()));

      }
    }
    // TODO(dan): should orphaned blocks be included, perhaps behind a flag?
  }
  return table.PrintTo(cout);
}
} // anonymous namespace

static unique_ptr<Mode> BuildFsDumpMode() {
  unique_ptr<Action> dump_cfile =
      ActionBuilder("cfile", &DumpCFile)
      .Description("Dump the contents of a CFile (column file)")
      .ExtraDescription("This interprets the contents of a CFile-formatted block "
                        "and outputs the decoded row data.")
      .AddRequiredParameter({ "block_id", "block identifier" })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("print_meta")
      .AddOptionalParameter("print_rows")
      .Build();

  unique_ptr<Action> dump_block =
      ActionBuilder("block", &DumpBlock)
      .Description("Dump the binary contents of a data block")
      .ExtraDescription("This performs no parsing or interpretation of the data stored "
                        "in the block but rather outputs its binary contents directly.")
      .AddRequiredParameter({ "block_id", "block identifier" })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Action> dump_tree =
      ActionBuilder("tree", &DumpFsTree)
      .Description("Dump the tree of a Kudu filesystem")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Action> dump_uuid =
      ActionBuilder("uuid", &DumpUuid)
      .Description("Dump the UUID of a Kudu filesystem")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  return ModeBuilder("dump")
      .Description("Dump a Kudu filesystem")
      .AddAction(std::move(dump_block))
      .AddAction(std::move(dump_cfile))
      .AddAction(std::move(dump_tree))
      .AddAction(std::move(dump_uuid))
      .Build();
}

unique_ptr<Mode> BuildFsMode() {
  unique_ptr<Action> check =
      ActionBuilder("check", &Check)
      .Description("Check a Kudu filesystem for inconsistencies")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("repair")
      .Build();

  unique_ptr<Action> format =
      ActionBuilder("format", &Format)
      .Description("Format a new Kudu filesystem")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("uuid")
      .Build();

  unique_ptr<Action> update =
      ActionBuilder("update_dirs", &Update)
      .Description("Updates the set of data directories in an existing Kudu filesystem")
      .ExtraDescription("If a data directory is in use by a tablet and is "
          "removed, the operation will fail unless --force is also used")
      .AddOptionalParameter("force", boost::none, string("If true, permits "
          "the removal of a data directory that is configured for use by "
          "existing tablets. Those tablets will fail the next time the server "
          "is started"))
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .Build();

  unique_ptr<Action> list =
      ActionBuilder("list", &List)
      .Description("List metadata for on-disk tablets, rowsets, blocks, and cfiles")
      .ExtraDescription("This tool is useful for discovering and gathering information about "
                        "on-disk data. Many field types can be added to the results with the "
                        "--columns flag, and results can be filtered to a specific table, "
                        "tablet, rowset, column, or block through flags.\n\n"
                        "Note: adding any of the 'cfile' fields to --columns will cause "
                        "the tool to read on-disk metadata for each CFile in the result set, "
                        "which could require large amounts of I/O when many results are returned.")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("table_id")
      .AddOptionalParameter("table_name")
      .AddOptionalParameter("tablet_id")
      .AddOptionalParameter("rowset_id")
      .AddOptionalParameter("column_id")
      .AddOptionalParameter("block_id")
      .AddOptionalParameter("columns", string("tablet-id, rowset-id, block-id, block-kind"),
                            Substitute("Comma-separated list of fields to include in output.\n"
                                       "Possible values: $0",
                                       JoinMapped(kFieldVariants, [] (Field field) {
                                                    return ToString(field);
                                                  }, ", ")))
      .AddOptionalParameter("format")
      .AddOptionalParameter("h")
      .Build();

  return ModeBuilder("fs")
      .Description("Operate on a local Kudu filesystem")
      .AddMode(BuildFsDumpMode())
      .AddAction(std::move(check))
      .AddAction(std::move(format))
      .AddAction(std::move(list))
      .AddAction(std::move(update))
      .Build();
}

} // namespace tools
} // namespace kudu
