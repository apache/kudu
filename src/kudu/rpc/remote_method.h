// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_RPC_REMOTE_METHOD_H_
#define KUDU_RPC_REMOTE_METHOD_H_

#include <string>

namespace kudu {
namespace rpc {

class RemoteMethodPB;

// Simple class that acts as a container for a fully qualified remote RPC name
// and converts to/from RemoteMethodPB.
// This class is also copyable and assignable for convenience reasons.
class RemoteMethod {
 public:
  RemoteMethod() {}
  RemoteMethod(const std::string& service_name, const std::string method_name);
  std::string service_name() const { return service_name_; }
  std::string method_name() const { return method_name_; }

  // Encode/decode to/from 'pb'.
  void FromPB(const RemoteMethodPB& pb);
  void ToPB(RemoteMethodPB* pb) const;

  std::string ToString() const;

 private:
  std::string service_name_;
  std::string method_name_;
};

} // namespace rpc
} // namespace kudu

#endif // KUDU_RPC_REMOTE_METHOD_H_
