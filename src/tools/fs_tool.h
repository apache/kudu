// Copyright (c) 2014, Cloudera, inc.
//
// Shared fields and methods for querying local files and directories
#ifndef KUDU_TOOLS_FS_TOOL_H
#define KUDU_TOOLS_FS_TOOL_H

#include <tr1/memory>
#include <iostream>
#include <vector>
#include <string>

#include "gutil/gscoped_ptr.h"
#include "util/status.h"

namespace kudu {

class FsManager;
class Schema;

namespace metadata {
class TabletMetadata;
class RowSetMetadata;
}

namespace tools {

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
  Status PrintLogSegmentHeader(const std::string& path);

  // Lists blocks for a tablet organized by rowset.
  Status ListBlocksForTablet(const std::string& tablet_id);

  // Lists blocks for all tablets.
  Status ListBlocksForAllTablets();

  // Prints the tablet metadata for a tablet 'tablet_id' with a master
  // block residing in 'master_block_path'. Will log an error but
  // return Status::OK() if the tablet id for the master block doesn't
  // equal 'tablet_id'.
  Status PrintTabletMeta(const std::string& master_block_path,
                         const std::string& tablet_id);

 private:

  Status ListSegmentsInDir(const std::string& segments_dir);

  Status ListBlocksInRowSet(const Schema& schema,
                            const metadata::RowSetMetadata& rs_meta);

  Status LoadTabletMetadata(const std::string& master_block_path,
                            const std::string& tablet_id,
                            gscoped_ptr<metadata::TabletMetadata> *meta);

  Status GetTabletsInMasterBlockDir(std::vector<std::string>* tablets);

  bool initialized_;
  const std::string base_dir_;
  const DetailLevel detail_level_;
  gscoped_ptr<FsManager> fs_manager_;
};

} // namespace tools
} // namespace kudu

#endif // KUDU_TOOLS_FS_TOOL_H
