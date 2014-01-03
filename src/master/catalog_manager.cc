// Copyright (c) 2013, Cloudera, inc.

#include "master/catalog_manager.h"

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>
#include <string>
#include <vector>

#include "master/master.h"
#include "master/sys_tables.h"
#include "master/ts_descriptor.h"

namespace kudu {
namespace master {

////////////////////////////////////////////////////////////
// Catalog Manager
////////////////////////////////////////////////////////////

CatalogManager::CatalogManager(Master *master)
  : master_(master) {
}

CatalogManager::~CatalogManager() {
}

Status CatalogManager::Init(bool is_first_run) {
  boost::lock_guard<LockType> l(lock_);

  sys_tables_.reset(new SysTablesTable(master_, master_->metric_registry()));
  sys_tablets_.reset(new SysTabletsTable(master_, master_->metric_registry()));

  if (is_first_run) {
    RETURN_NOT_OK(sys_tables_->CreateNew(master_->fs_manager()));
    RETURN_NOT_OK(sys_tablets_->CreateNew(master_->fs_manager()));
  } else {
    RETURN_NOT_OK(sys_tables_->Load(master_->fs_manager()));
    RETURN_NOT_OK(sys_tablets_->Load(master_->fs_manager()));

    // TODO: Load tables in-memory
  }

  // TODO: Run the cleaner

  return Status::OK();
}

////////////////////////////////////////////////////////////
// TabletInfo
////////////////////////////////////////////////////////////

TabletInfo::TabletInfo(TableInfo *table, const std::string& tablet_id)
  : tablet_id_(tablet_id), table_(table),
    last_update_ts_(MonoTime::Now(MonoTime::FINE)),
    current_metadata_(&metadata_) {
}

TabletInfo::~TabletInfo() {
  if (!is_committed()) {
    delete current_metadata_;
  }
}

void TabletInfo::AddReplica(TSDescriptor* ts_desc) {
  last_update_ts_ = MonoTime::Now(MonoTime::FINE);
  BOOST_FOREACH(const TSDescriptor* l, locations_) {
    if (l == ts_desc) return;
  }
  VLOG(2) << tablet_id_ << " reported on " << ts_desc->permanent_uuid();
  locations_.push_back(ts_desc);
}

void TabletInfo::ClearReplicasOnTS(const TSDescriptor* ts) {
  std::vector<TSDescriptor*>::iterator it = locations_.begin();
  while (it != locations_.end()) {
    if (*it == ts) {
      it = locations_.erase(it);
    } else {
      ++it;
    }
  }
}

void TabletInfo::set_state(SysTabletsEntryPB::State state, const string& msg) {
  SysTabletsEntryPB *metadata = mutable_metadata();
  metadata->set_state(state);
  metadata->set_state_msg(msg);
}

SysTabletsEntryPB *TabletInfo::mutable_metadata() {
  if (is_committed()) {
    // Keep a copy of the old committed as current
    current_metadata_ = new SysTabletsEntryPB(metadata_);
  }
  // Return the metadata for edit
  return &metadata_;
}

bool TabletInfo::is_committed() const {
  return current_metadata_ == &metadata_;
}

void TabletInfo::Commit() {
  DCHECK(!is_committed());

  // Delete the old Metadata
  delete current_metadata_;
  // Make the updated metadata the current one
  current_metadata_ = &metadata_;
}

////////////////////////////////////////////////////////////
// TableInfo
////////////////////////////////////////////////////////////

TableInfo::TableInfo(const std::string& table_id)
  : table_id_(table_id), current_metadata_(&metadata_) {
}

TableInfo::~TableInfo() {
  if (!is_committed()) {
    delete current_metadata_;
  }
}

void TableInfo::AddTablet(TabletInfo *tablet) {
  tablet_map_[tablet->metadata().start_key()] = tablet;
}

void TableInfo::AddTablets(const vector<TabletInfo*>& tablets) {
  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    tablet_map_[tablet->metadata().start_key()] = tablet;
  }
}

void TableInfo::set_state(SysTablesEntryPB::State state, const string& msg) {
  SysTablesEntryPB *metadata = mutable_metadata();
  metadata->set_state(state);
  metadata->set_state_msg(msg);
}

SysTablesEntryPB *TableInfo::mutable_metadata() {
  if (is_committed()) {
    // Keep a copy of the old committed as current
    current_metadata_ = new SysTablesEntryPB(metadata_);
  }
  // Return the metadata for edit
  return &metadata_;
}

bool TableInfo::is_committed() const {
  return current_metadata_ == &metadata_;
}

void TableInfo::Commit() {
  DCHECK(!is_committed());

  // Delete the old Metadata
  delete current_metadata_;
  // Make the updated metadata the current one
  current_metadata_ = &metadata_;
}

} // namespace master
} // namespace kudu
