// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_SYS_TABLES_H_
#define KUDU_MASTER_SYS_TABLES_H_

#include <string>
#include <vector>

#include "master/master.pb.h"
#include "server/metadata.h"
#include "server/oid_generator.h"
#include "tablet/tablet_peer.h"
#include "util/status.h"

namespace kudu {

class Schema;
class FsManager;

namespace tserver {
class WriteRequestPB;
class WriteResponsePB;
}

namespace master {
class Master;

class Master;
class TableInfo;
class TabletInfo;

// Abstract class for a sys-table.
// - The sys-table has only one tablet.
// - The sys-table is managed by the master and not exposed to the user
//   as a "normal table", instead we have Master APIs to query the table.
class SysTable {
 public:
  SysTable(Master* master,
           MetricRegistry* metrics,
           const string& name);

  virtual ~SysTable() {}

  // Allow for orderly shutdown of tablet peer, etc.
  virtual void Shutdown();

  // Load the Metadata from disk, and initialize the TabletPeer for the sys-table
  Status Load(FsManager *fs_manager);

  // Create the new Metadata and initialize the TabletPeer for the sys-table.
  Status CreateNew(FsManager *fs_manager);

 protected:
  virtual const char *table_name() const = 0;

  // Return the schema of the table.
  // NOTE: This is the "server-side" schema, so it must have the column IDs.
  virtual Schema BuildTableSchema() = 0;

  // Setup the 'master_block' with the IDs of the super-blocks for
  // the tablet of the sys-table.
  virtual void SetupTabletMasterBlock(metadata::TabletMasterBlockPB *master_block) = 0;

  // Returns 'Status::OK()' if the WriteTranasction completed
  Status SyncWrite(const tserver::WriteRequestPB *req, tserver::WriteResponsePB *resp);

  void SysTableStateChanged(tablet::TabletPeer* tablet_peer);

  // Table schema, without IDs, used to send messages to the TabletPeer
  Schema schema_;
  Schema key_schema_;

  MetricContext metric_ctx_;

  gscoped_ptr<TaskExecutor> leader_apply_executor_;
  gscoped_ptr<TaskExecutor> replica_apply_executor_;

  scoped_refptr<tablet::TabletPeer> tablet_peer_;

 private:
  Status SetupTablet(const scoped_refptr<metadata::TabletMetadata>& metadata,
                     const metadata::QuorumPeerPB& quorum_peer);

  Master* master_;
};

// The "sys.tablets" table is the table used to keep tracks of the tables tablets.
class SysTabletsTable : public SysTable {
 public:
  class Visitor {
   public:
    virtual Status VisitTablet(const std::string& table_id,
                               const std::string& tablet_id,
                               const SysTabletsEntryPB& metadata) = 0;
  };

  SysTabletsTable(Master* master, MetricRegistry* metrics)
    : SysTable(master, metrics, table_name()) {
  }

  Status AddTablets(const vector<TabletInfo*>& tablets);
  Status UpdateTablets(const vector<TabletInfo*>& tablets);
  Status AddAndUpdateTablets(const vector<TabletInfo*>& tablets_to_add,
                             const vector<TabletInfo*>& tablets_to_update);
  Status DeleteTablets(const vector<TabletInfo*>& tablets);

  // full scan of the table
  Status VisitTablets(Visitor *visitor);

 protected:
  virtual const char *table_name() const OVERRIDE { return "sys.tablets"; }

  virtual Schema BuildTableSchema() OVERRIDE;
  virtual void SetupTabletMasterBlock(metadata::TabletMasterBlockPB *master_block) OVERRIDE;

 private:
  DISALLOW_COPY_AND_ASSIGN(SysTabletsTable);

  Status VisitTabletFromRow(const RowBlockRow& row, Visitor *visitor);
  Status AddTabletsToPB(const std::vector<TabletInfo*>& tablets,
                        RowOperationsPB::Type op_type,
                        RowOperationsPB* ops) const;
};

// The "sys.tables" table is the table that contains the table schema and other
// table metadata information (like the name or settings)
class SysTablesTable : public SysTable {
 public:
  class Visitor {
   public:
    virtual Status VisitTable(const std::string& table_id,
                              const SysTablesEntryPB& metadata) = 0;
  };

  SysTablesTable(Master* master, MetricRegistry* metrics)
    : SysTable(master, metrics, table_name()) {
  }

  Status AddTable(const TableInfo *table);
  Status UpdateTable(const TableInfo *table);
  Status DeleteTable(const TableInfo *table);

  // full scan of the table
  Status VisitTables(Visitor *visitor);

 protected:
  virtual const char *table_name() const OVERRIDE { return "sys.tables"; }

  virtual Schema BuildTableSchema() OVERRIDE;
  virtual void SetupTabletMasterBlock(metadata::TabletMasterBlockPB *master_block) OVERRIDE;

 private:
  DISALLOW_COPY_AND_ASSIGN(SysTablesTable);

  Status VisitTableFromRow(const RowBlockRow& row, Visitor *visitor);
};

} // namespace master
} // namespace kudu

#endif
