// Copyright (c) 2014 Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TSERVER_REMOTE_BOOTSTRAP_SERVICE_H_
#define KUDU_TSERVER_REMOTE_BOOTSTRAP_SERVICE_H_

#include <tr1/unordered_map>
#include <string>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tserver/remote_bootstrap.service.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

namespace kudu {
class FsManager;

namespace log {
class ReadableLogSegment;
} // namespace log

namespace tserver {

class RemoteBootstrapSession;
class TabletPeerLookupIf;

class RemoteBootstrapServiceImpl : public RemoteBootstrapServiceIf {
 public:
  RemoteBootstrapServiceImpl(FsManager* fs_manager,
                             TabletPeerLookupIf* tablet_peer_lookup,
                             const scoped_refptr<MetricEntity>& metric_entity);

  virtual void BeginRemoteBootstrapSession(const BeginRemoteBootstrapSessionRequestPB* req,
                                           BeginRemoteBootstrapSessionResponsePB* resp,
                                           rpc::RpcContext* context) OVERRIDE;

  virtual void CheckSessionActive(const CheckRemoteBootstrapSessionActiveRequestPB* req,
                                  CheckRemoteBootstrapSessionActiveResponsePB* resp,
                                  rpc::RpcContext* context) OVERRIDE;

  virtual void FetchData(const FetchDataRequestPB* req,
                         FetchDataResponsePB* resp,
                         rpc::RpcContext* context) OVERRIDE;

  virtual void EndRemoteBootstrapSession(const EndRemoteBootstrapSessionRequestPB* req,
                                         EndRemoteBootstrapSessionResponsePB* resp,
                                         rpc::RpcContext* context) OVERRIDE;

  virtual void Shutdown() OVERRIDE;

 private:
  typedef std::tr1::unordered_map<std::string, scoped_refptr<RemoteBootstrapSession> > SessionMap;
  typedef std::tr1::unordered_map<std::string, MonoTime> MonoTimeMap;

  // Look up session in session map.
  Status FindSessionUnlocked(const std::string& session_id,
                             RemoteBootstrapErrorPB::Code* app_error,
                             scoped_refptr<RemoteBootstrapSession>* session) const;

  // Validate the data identifier in a FetchData request.
  Status ValidateFetchRequestDataId(const DataIdPB& data_id,
                                    RemoteBootstrapErrorPB::Code* app_error,
                                    const scoped_refptr<RemoteBootstrapSession>& session) const;

  // Take note of session activity; Re-update the session timeout deadline.
  void ResetSessionExpirationUnlocked(const std::string& session_id);

  // Destroy the specified remote bootstrap session.
  Status DoEndRemoteBootstrapSessionUnlocked(const std::string& session_id,
                                             RemoteBootstrapErrorPB::Code* app_error);

  // The timeout thread periodically checks whether sessions are expired and
  // removes them from the map.
  void EndExpiredSessions();

  FsManager* fs_manager_;
  TabletPeerLookupIf* tablet_peer_lookup_;

  // Protects sessions_ and session_expirations_ maps.
  mutable simple_spinlock sessions_lock_;
  SessionMap sessions_;
  MonoTimeMap session_expirations_;

  // Session expiration thread.
  // TODO: this is a hack, replace with some kind of timer impl. See KUDU-286.
  CountDownLatch shutdown_latch_;
  scoped_refptr<Thread> session_expiration_thread_;
};

} // namespace tserver
} // namespace kudu

#endif // KUDU_TSERVER_REMOTE_BOOTSTRAP_SERVICE_H_
