// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.
#ifndef KUDU_RPC_NEGOTIATION_H
#define KUDU_RPC_NEGOTIATION_H

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/monotime.h"

namespace kudu {
namespace rpc {

class Connection;

class Negotiation {
 public:
  static void RunNegotiation(const scoped_refptr<Connection>& conn,
                             const MonoTime &deadline);
 private:
  DISALLOW_IMPLICIT_CONSTRUCTORS(Negotiation);
};

} // namespace rpc
} // namespace kudu
#endif // KUDU_RPC_NEGOTIATION_H
