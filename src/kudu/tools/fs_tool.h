// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
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

namespace tablet {
class TabletMetadata;
class RowSetMetadata;
}

namespace tools {

struct DumpOptions {
  std::string start_key;
  std::string end_key;
  size_t nrows;
  bool metadata_only;

  DumpOptions()
      : start_key(""),
        end_key(""),
        nrows(0),
        metadata_only(false) {
  }
};

class FsTool {
 public:

  enum DetailLevel {
    MINIMUM = 0, // Minimum amount of information
    HEADERS_ONLY = 1, // Tablet/segment headers only
    MAXIMUM = 2,
  };

  explicit FsTool(DetailLevel detail_level);
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
  Status PrintTabletMeta(const std::string& tablet_id, int indent);

  // Dumps the blocks that make up a tablet, rowset by rowset. This ends up
  // outputting on a column-by-column basis, as close as possible to the raw
  // storage. See also: DumpRowSet().
  Status DumpTabletBlocks(const std::string& tablet_id,
                    const DumpOptions& opts,
                    int indent);

  // Dump the data stored in a tablet. The output here is much more readable
  // than DumpTabletBlocks, since it reconstructs rows and associates undo/redo deltas
  // with those rows.
  Status DumpTabletData(const std::string& tablet_id);

  // Dumps column blocks, all types of delta blocks for a given
  // rowset.
  Status DumpRowSet(const std::string& tablet_id,
                    int64_t rowset_id,
                    const DumpOptions& opts,
                    int indent);

  Status DumpCFileBlock(const std::string& block_id,
                        const DumpOptions& opts,
                        int indent);

  // Prints the server's UUID to whom the data belongs and nothing else.
  Status PrintUUID(int indent);
 private:
  Status ListSegmentsInDir(const std::string& segments_dir);

  Status ListBlocksInRowSet(const Schema& schema,
                            const tablet::RowSetMetadata& rs_meta);

  Status DumpRowSetInternal(const Schema& schema,
                            const std::tr1::shared_ptr<tablet::RowSetMetadata>& rs_meta,
                            const DumpOptions& opts,
                            int indent);

  Status DumpCFileBlockInternal(const BlockId& block_id,
                                const DumpOptions& opts,
                                int indent);

  Status DumpDeltaCFileBlockInternal(const Schema& schema,
                                     const std::tr1::shared_ptr<tablet::RowSetMetadata>& rs_meta,
                                     const BlockId& block_id,
                                     tablet::DeltaType delta_type,
                                     const DumpOptions& opts,
                                     int indent,
                                     bool metadata_only);

  Status OpenBlockAsFile(const BlockId& block_id,
                         uint64_t* file_size,
                         std::tr1::shared_ptr<RandomAccessFile>* block_reader);

  bool initialized_;
  const DetailLevel detail_level_;
  gscoped_ptr<FsManager> fs_manager_;
};

} // namespace tools
} // namespace kudu

#endif // KUDU_TOOLS_FS_TOOL_H
