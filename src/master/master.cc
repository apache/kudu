// Copyright (c) 2013, Cloudera, inc.

#include "master/master.h"

#include <boost/foreach.hpp>
#include <list>
#include <vector>

#include "rpc/messenger.h"
#include "rpc/service_if.h"
#include "rpc/service_pool.h"
#include "server/rpc_server.h"
#include "master/master_service.h"
#include "master/ts_manager.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using std::vector;
using kudu::rpc::ServiceIf;

namespace kudu {
namespace master {

Master::Master(const MasterOptions& opts)
  : ServerBase(opts.rpc_opts, opts.webserver_opts),
    initted_(false),
    ts_manager_(new TSManager()) {
}

Master::~Master() {
}

string Master::ToString() const {
  // TODO: include port numbers, etc.
  return "Master";
}

Status Master::Init() {
  CHECK(!initted_);
  RETURN_NOT_OK(ServerBase::Init());
  initted_ = true;
  return Status::OK();
}

Status Master::Start() {
  CHECK(initted_);

  gscoped_ptr<ServiceIf> impl(new MasterServiceImpl(this));
  RETURN_NOT_OK(ServerBase::Start(impl.Pass()));

  return Status::OK();
}

} // namespace master
} // namespace kudu
