// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#ifndef KUDU_RPC_NEGOTIATION_H
#define KUDU_RPC_NEGOTIATION_H

#include <tr1/memory>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/monotime.h"
#include "kudu/util/task_executor.h"

namespace kudu {

class Status;

namespace rpc {

class Connection;
class SaslClient;
class SaslServer;

// Handle client-side blocking connection negotiation, including SASL negotiation and
// sending the ConnectionContextPB.
class ClientNegotiationTask : public kudu::Task {
 public:
  ClientNegotiationTask(const scoped_refptr<Connection>& conn, const MonoTime &deadline);
  virtual kudu::Status Run() OVERRIDE;
  virtual bool Abort() OVERRIDE;
 private:
  scoped_refptr<Connection> conn_;
  MonoTime deadline_;
};

// Handle server-side blocking connection negotiation, including SASL negotiation and
// receiving / validating the ConnectionContextPB.
class ServerNegotiationTask : public kudu::Task {
 public:
  explicit ServerNegotiationTask(const scoped_refptr<Connection>& conn,
                                 const MonoTime &deadline);
  virtual kudu::Status Run() OVERRIDE;
  virtual bool Abort() OVERRIDE;
 private:
  scoped_refptr<Connection> conn_;
  MonoTime deadline_;
};

// Return control of the connection back to the Reactor.
class NegotiationCallback : public FutureCallback {
 public:
  explicit NegotiationCallback(const scoped_refptr<Connection>& conn);
  virtual void OnSuccess() OVERRIDE;
  virtual void OnFailure(const kudu::Status& status) OVERRIDE;
 private:
  scoped_refptr<Connection> conn_;
};

} // namespace rpc
} // namespace kudu
#endif // KUDU_RPC_NEGOTIATION_H
