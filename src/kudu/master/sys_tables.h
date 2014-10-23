// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_SYS_TABLES_H_
#define KUDU_MASTER_SYS_TABLES_H_

#include <string>
#include <vector>

#include "kudu/master/master.pb.h"
#include "kudu/server/metadata.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/status.h"

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
struct MasterOptions;
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

  virtual ~SysTable();

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
  virtual void SetupTabletMasterBlock(tablet::TabletMasterBlockPB *master_block) = 0;

  // Returns 'Status::OK()' if the WriteTranasction completed
  Status SyncWrite(const tserver::WriteRequestPB *req, tserver::WriteResponsePB *resp);

  void SysTableStateChanged(tablet::TabletPeer* tablet_peer);

  // Table schema, without IDs, used to send messages to the TabletPeer
  Schema schema_;
  Schema key_schema_;

  MetricContext metric_ctx_;

  gscoped_ptr<ThreadPool> leader_apply_pool_;
  gscoped_ptr<ThreadPool> replica_apply_pool_;

  scoped_refptr<tablet::TabletPeer> tablet_peer_;

 private:
  friend class CatalogManager;

  Status SetupTablet(const scoped_refptr<tablet::TabletMetadata>& metadata);

  // Use the master options to generate a new quorum.
  // In addition, resolve all UUIDs of this quorum.
  //
  // Note: The current node adds itself to the quorum whether leader or
  // follower, depending on whether the Master options leader flag is
  // set. Even if the local node should be a follower, it should not be listed
  // in the Master options followers list, as it will add itself automatically.
  //
  // TODO: Revisit this whole thing when integrating leader election.
  Status SetupDistributedQuorum(const MasterOptions& options, int64_t seqno,
                                metadata::QuorumPB* quorum);

  const scoped_refptr<tablet::TabletPeer>& tablet_peer() const {
    return tablet_peer_;
  }

  std::string tablet_id() const {
    return tablet_peer_->tablet_id();
  }

  // Waits for the tablet to reach 'RUNNING' state.
  //
  // Contrary to tablet servers, in master we actually wait for the master tablet
  // to become online synchronously, this allows us to fail fast if something fails
  // and shouldn't induce the all-workers-blocked-waiting-for-tablets problem
  // that we've seen in tablet servers since the master only has to boot a few
  // tablets.
  Status WaitUntilRunning();

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
  virtual void SetupTabletMasterBlock(tablet::TabletMasterBlockPB *master_block) OVERRIDE;

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
  virtual void SetupTabletMasterBlock(tablet::TabletMasterBlockPB *master_block) OVERRIDE;

 private:
  DISALLOW_COPY_AND_ASSIGN(SysTablesTable);

  Status VisitTableFromRow(const RowBlockRow& row, Visitor *visitor);
};

} // namespace master
} // namespace kudu

#endif
