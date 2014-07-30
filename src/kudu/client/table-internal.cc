// Copyright (c) 2014, Cloudera, inc.

#include "kudu/client/table-internal.h"

#include <string>

#include "kudu/client/client-internal.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/rpc_controller.h"

namespace kudu {

using master::GetTableLocationsRequestPB;
using master::GetTableLocationsResponsePB;
using std::string;
using std::tr1::shared_ptr;

namespace client {

KuduTable::Data::Data(const shared_ptr<KuduClient>& client,
                      const string& name,
                      const KuduSchema& schema)
  : client_(client),
    name_(name),
    schema_(schema) {
}

KuduTable::Data::~Data() {
}

Status KuduTable::Data::Open() {
  // TODO: fetch the schema from the master here once catalog is available.
  GetTableLocationsRequestPB req;
  GetTableLocationsResponsePB resp;

  req.mutable_table()->set_table_name(name_);
  do {
    rpc::RpcController rpc;
    rpc.set_timeout(client_->default_admin_operation_timeout());
    RETURN_NOT_OK(client_->data_->master_proxy_->GetTableLocations(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }

    if (resp.tablet_locations_size() > 0)
      break;

    /* TODO: Add a timeout or number of retries */
    usleep(100000);
  } while (1);

  VLOG(1) << "Open Table " << name_ << ", found " << resp.tablet_locations_size() << " tablets";
  return Status::OK();
}

} // namespace client
} // namespace kudu
