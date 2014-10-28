// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_SERVER_RPCZ_PATH_HANDLER_H
#define KUDU_SERVER_RPCZ_PATH_HANDLER_H

#include <tr1/memory>

namespace kudu {

namespace rpc {
class Messenger;
} // namespace rpc

class Webserver;

void AddRpczPathHandlers(const std::tr1::shared_ptr<rpc::Messenger>& messenger,
                         Webserver* webserver);

} // namespace kudu
#endif /* KUDU_SERVER_RPCZ_PATH_HANDLER_H */
