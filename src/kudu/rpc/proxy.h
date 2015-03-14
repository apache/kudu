// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_RPC_PROXY_H
#define KUDU_RPC_PROXY_H

#include <tr1/memory>
#include <string>

#include "kudu/gutil/atomicops.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

namespace google {
namespace protobuf {
  class Message;
}
}

namespace kudu {
namespace rpc {

class Messenger;

// Interface to send calls to a remote service.
//
// Proxy objects do not map one-to-one with TCP connections.  The underlying TCP
// connection is not established until the first call, and may be torn down and
// re-established as necessary by the messenger. Additionally, the messenger is
// likely to multiplex many Proxy objects on the same connection.
//
// Proxy objects are thread-safe after initialization only.
// Setters on the Proxy are not thread-safe, and calling a setter after any RPC
// request has started will cause a fatal error.
//
// After initialization, multiple threads may make calls using the same proxy object.
class Proxy {
 public:
  Proxy(const std::tr1::shared_ptr<Messenger>& messenger,
        const Sockaddr& remote,
        const std::string& service_name);
  ~Proxy();

  // Call a remote method asynchronously.
  //
  // Typically, users will not call this directly, but rather through
  // a generated Proxy subclass.
  //
  // method: the method name to invoke on the remote server.
  //
  // req:  the request protobuf. This will be serialized immediately,
  //       so the caller may free or otherwise mutate 'req' safely.
  //
  // resp: the response protobuf. This protobuf will be mutated upon
  //       completion of the call. The RPC system does not take ownership
  //       of this storage.
  //
  // NOTE: 'req' and 'resp' should be the appropriate protocol buffer implementation
  // class corresponding to the parameter and result types of the service method
  // defined in the service's '.proto' file.
  //
  // controller: the RpcController to associate with this call. Each call
  //             must use a unique controller object. Does not take ownership.
  //
  // callback: the callback to invoke upon call completion. This callback may
  //           be invoked before AsyncRequest() itself returns, or any time
  //           thereafter. It may be invoked either on the caller's thread
  //           or by an RPC IO thread, and thus should take care to not
  //           block or perform any heavy CPU work.
  void AsyncRequest(const std::string& method,
                    const google::protobuf::Message& req,
                    google::protobuf::Message* resp,
                    RpcController* controller,
                    const ResponseCallback& callback) const;

  // The same as AsyncRequest(), except that the call blocks until the call
  // finishes. If the call fails, returns a non-OK result.
  Status SyncRequest(const std::string& method,
                     const google::protobuf::Message& req,
                     google::protobuf::Message* resp,
                     RpcController* controller) const;

  // Set the user credentials which should be used to log in.
  void set_user_credentials(const UserCredentials& user_credentials);

  // Get the user credentials which should be used to log in.
  const UserCredentials& user_credentials() const { return conn_id_.user_credentials(); }

 private:
  const std::string service_name_;
  std::tr1::shared_ptr<Messenger> messenger_;
  ConnectionId conn_id_;
  mutable Atomic32 is_started_;

  DISALLOW_COPY_AND_ASSIGN(Proxy);
};

} // namespace rpc
} // namespace kudu

#endif
