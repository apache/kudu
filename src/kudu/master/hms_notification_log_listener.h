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

#pragma once

#include <cstdint>
#include <vector>

#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

namespace hive {
class NotificationEvent;
}

namespace kudu {

class MonoTime;
class Thread;

namespace master {

class CatalogManager;

// A CatalogManager background task which listens for events occurring in the
// Hive Metastore, and synchronizes the Kudu catalog accordingly.
//
// As a background task, the lifetime of an instance of this class must be less
// than the catalog manager it belongs to.
//
// The notification log listener task continuously wakes up according to its
// configured poll period, however it performs no work when the master is a
// follower.
//
// When a change to the Kudu catalog is performed in response to a notification
// log event, the corresponding event ID is recorded in the sys catalog as the
// latest handled event. This ensures that masters do not double-apply
// notification events as leadership changes.
//
// The notification log listener listens for two types of events on Kudu tables:
//
// - ALTER TABLE RENAME
//    Table rename is a special case of ALTER TABLE. The notification log
//    listener listens for rename event notifications for Kudu tables, and
//    renames the corresponding Kudu table. See below for why renames can be
//    applied back to Kudu, but not other types of alterations.
//
// - DROP TABLE
//    The notification log listener listens for drop table events for Kudu
//    tables, and drops the corresponding Kudu table. This allows the catalogs
//    to stay synchronized when DROP TABLE and DROP DATABASE CASCADE Hive
//    commands are executed.
//
// The notification log listener can support renaming and dropping tables in a
// safe manner because the Kudu table ID is stored in the HMS table entry. Using
// the Kudu table ID, the exact table which the event applies to can always be
// identified. For other changes made in ALTER TABLE statements, such as ALTER
// TABLE DROP COLUMN, there is no way to identify with certainty which column
// has been dropped, since we do not store column IDs in the HMS table entries.
class HmsNotificationLogListenerTask {
 public:

  explicit HmsNotificationLogListenerTask(CatalogManager* catalog_manager);
  ~HmsNotificationLogListenerTask();

  // Initializes the HMS notification log listener. When invoking this method,
  // the catalog manager must be in the process of initializing.
  Status Init() WARN_UNUSED_RESULT;

  // Shuts down the HMS notification log listener. This must be called before
  // shutting down the catalog manager.
  void Shutdown();

  // Waits for the notification log listener to process the latest notification
  // log event.
  //
  // Note: an error will be returned if the listener is unable to retrieve the
  // latest notifications from the HMS. If individual notifications are unable
  // to be processed, no error will be returned.
  Status WaitForCatchUp(const MonoTime& deadline) WARN_UNUSED_RESULT;

 private:

  // Runs the main loop of the listening thread.
  void RunLoop();

  // Polls the Hive Metastore for notification events, and handle them.
  Status Poll();

  // Handles an ALTER TABLE event. Must only be called on the listening thread.
  //
  // The event is parsed, and if it is a rename table event for a Kudu table,
  // the table is renamed in the local catalog.  All other events are ignored.
  Status HandleAlterTableEvent(const hive::NotificationEvent& event,
                               int64_t* durable_event_id) WARN_UNUSED_RESULT;

  // Handles a DROP TABLE event. Must only be called on the listening thread.
  //
  // The event is parsed, and if it is a drop table event for a Kudu table, the
  // table is deleted in the local catalog. All other events are ignored.
  Status HandleDropTableEvent(const hive::NotificationEvent& event,
                              int64_t* durable_event_id) WARN_UNUSED_RESULT;

  // The associated catalog manager.
  //
  // May be initialized to nullptr in the constructor to facilitate unit
  // testing. In this case all interactions with the catalog manager and HMS
  // are skipped.
  CatalogManager* catalog_manager_;

  // The listening thread.
  scoped_refptr<kudu::Thread> thread_;

  // Protects access to fields below.
  mutable Mutex lock_;

  // Set to true if the task is in the process of shutting down.
  //
  // Protected by lock_.
  bool closing_;

  // Manages waking the notification log listener thread when the catalog
  // manager needs to ensure that all recent notification log events have been
  // handled.
  //
  // Protected by lock_.
  ConditionVariable wake_up_cv_;

  // Queue of callbacks to execute when the notification log listener is caught
  // up. These callbacks enable the catalog manager to wait for the notification
  // log listener to have processed the latest events before proceeding with
  // metadata ops involving the HMS table namespace.
  //
  // Protected by lock_.
  std::vector<StatusCallback> catch_up_callbacks_;
};

} // namespace master
} // namespace kudu
