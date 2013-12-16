// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_SYS_TABLES_H_
#define KUDU_MASTER_SYS_TABLES_H_

#include <string>
#include <vector>

#include "server/metadata.h"
#include "server/oid_generator.h"
#include "tablet/tablet_peer.h"

namespace kudu {

class Schema;
class FsManager;

namespace master {

// Abstract class for a sys-table.
// - The sys-table has only one tablet.
// - The sys-table is managed by the master and not exposed to the user
//   as a "normal table", instead we have Master APIs to query the table.
class SysTable {
 public:
  SysTable(MetricRegistry* metrics, const string& name);
  virtual ~SysTable() {}

  // Load the Metadata from disk, and initialize the TabletPeer for the sys-table
  Status Load(FsManager *fs_manager);

  // Create the new Metadata and initialize the TabletPeer for the sys-table.
  Status CreateNew(FsManager *fs_manager);

 protected:
  // Return the schema of the table.
  // NOTE: This is the "server-side" schema, so it must have the column IDs.
  virtual Schema BuildTableSchema() = 0;

  // Setup the 'master_block' with the IDs of the super-blocks for
  // the tablet of the sys-table.
  virtual void SetupTabletMasterBlock(metadata::TabletMasterBlockPB *master_block) = 0;

  // Table schema, without IDs, used to send messages to the TabletPeer
  Schema schema_;

  MetricContext metric_ctx_;
  gscoped_ptr<tablet::TabletPeer> tablet_peer_;

 private:
  Status SetupTablet(gscoped_ptr<metadata::TabletMetadata> metadata);
};

// The "sys.tablets" table is the table used to keep tracks of the tables tablets.
class SysTabletsTable : public SysTable {
 public:
  explicit SysTabletsTable(MetricRegistry* metrics)
    : SysTable(metrics, "sys.tablets") {
  }

 protected:
  Schema BuildTableSchema();
  void SetupTabletMasterBlock(metadata::TabletMasterBlockPB *master_block);

 private:
  DISALLOW_COPY_AND_ASSIGN(SysTabletsTable);

  ObjectIdGenerator oid_generator_;
};

// The "sys.tables" table is the table that contains the table schema and other
// table metadata information (like the name or settings)
class SysTablesTable : public SysTable {
 public:
  explicit SysTablesTable(MetricRegistry* metrics)
    : SysTable(metrics, "sys.tables") {
  }

  Status AddTable(const string& name,
                  const metadata::TableDescriptorPB& desc,
                  const std::tr1::shared_ptr<FutureCallback>& callback);

 protected:
  Schema BuildTableSchema();
  void SetupTabletMasterBlock(metadata::TabletMasterBlockPB *master_block);

 private:
  DISALLOW_COPY_AND_ASSIGN(SysTablesTable);

  ObjectIdGenerator oid_generator_;
};

} // namespace master
} // namespace kudu

#endif
