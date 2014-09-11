// Copyright (c) 2014, Cloudera, inc.
//
// Shared fields and methods for querying local files and directories
#ifndef KUDU_TOOLS_FS_TOOL_H
#define KUDU_TOOLS_FS_TOOL_H

#include <tr1/memory>
#include <iostream>
#include <vector>
#include <string>
#include <utility>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/status.h"
#include "kudu/tablet/delta_key.h"

namespace kudu {

class FsManager;
class Schema;
class BlockId;
class RandomAccessFile;

namespace metadata {
class TabletMetadata;
class RowSetMetadata;
}

namespace tools {

struct DumpOptions {
  std::string start_key;
  std::string end_key;
  size_t nrows;

  DumpOptions()
      : start_key(""), end_key(""), nrows(0) {
  }
};

class FsTool {
 public:

  enum DetailLevel {
    MINIMUM = 0, // Minimum amount of information
    HEADERS_ONLY = 1, // Tablet/segment headers only
    MAXIMUM = 2,
  };

  FsTool(const std::string& base_dir, DetailLevel detail_level);
  ~FsTool();

  Status Init();

  // Prints out the file system tree.
  Status FsTree();

  // Lists all log segments in the root WALs directory.
  Status ListAllLogSegments();

  // Lists all log segments for tablet 'tablet_id'.
  Status ListLogSegmentsForTablet(const std::string& tablet_id);

  // Lists all tablets in a tablet server's local file system.
  Status ListAllTablets();

  // Prints the header for a log segment residing in 'path'.
  Status PrintLogSegmentHeader(const std::string& path, int indent);

  // Lists blocks for a tablet organized by rowset.
  Status ListBlocksForTablet(const std::string& tablet_id);

  // Lists blocks for all tablets.
  Status ListBlocksForAllTablets();

  // Prints the tablet metadata for a tablet 'tablet_id'.
  Status PrintTabletMeta(const std::string& tablet_id,
                         int indent);

  // Dumps all of the rowset in tablet. See also: DumpRowSet().
  Status DumpTablet(const std::string& tablet_id,
                    const DumpOptions& opts,
                    int indent);

  // Dumps column blocks, all types of delta blocks for a given
  // rowset.
  Status DumpRowSet(const std::string& tablet_id,
                    size_t rowset_idx,
                    const DumpOptions& opts,
                    int indent);

  Status DumpCFileBlock(const std::string& block_id,
                        const DumpOptions& opts,
                        int indent);

 private:
  Status ListSegmentsInDir(const std::string& segments_dir);

  Status ListBlocksInRowSet(const Schema& schema,
                            const metadata::RowSetMetadata& rs_meta);

  Status LoadTabletMetadata(const std::string& master_block_path,
                            const std::string& tablet_id,
                            scoped_refptr<metadata::TabletMetadata> *meta);

  Status GetTabletsInMasterBlockDir(std::vector<std::string>* tablets);

  Status DumpRowSetInternal(const Schema& schema,
                            const std::tr1::shared_ptr<metadata::RowSetMetadata>& rs_meta,
                            const DumpOptions& opts,
                            int indent);

  Status GetMasterBlockPath(const std::string& tablet_id,
                            std::string* master_block_path);

  Status DumpCFileBlockInternal(const BlockId& block_id,
                                const DumpOptions& opts,
                                int indent);

  Status DumpDeltaCFileBlockInternal(const Schema& schema,
                                     const std::tr1::shared_ptr<metadata::RowSetMetadata>& rs_meta,
                                     const BlockId& block_id,
                                     tablet::DeltaType delta_type,
                                     const DumpOptions& opts,
                                     int indent);

  Status PrintTabletMetaInternal(const std::string& master_block_path,
                                 const std::string& tablet_id,
                                 int indent);

  Status OpenBlockAsFile(const BlockId& block_id,
                         uint64_t* file_size,
                         std::tr1::shared_ptr<RandomAccessFile>* block_reader);

  bool initialized_;
  const std::string base_dir_;
  const DetailLevel detail_level_;
  gscoped_ptr<FsManager> fs_manager_;
};

} // namespace tools
} // namespace kudu

#endif // KUDU_TOOLS_FS_TOOL_H
