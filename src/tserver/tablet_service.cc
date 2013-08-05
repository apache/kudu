// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tablet_service.h"

#include <tr1/memory>
#include <vector>

#include "common/wire_protocol.h"
#include "tablet/tablet.h"
#include "tserver/tablet_server.h"
#include "tserver/tserver.pb.h"
#include "util/status.h"

using kudu::tablet::Tablet;
using std::tr1::shared_ptr;
using std::vector;
using google::protobuf::RepeatedPtrField;

namespace kudu {
namespace tserver {

TabletServiceImpl::TabletServiceImpl(TabletServer* server)
  : server_(server) {
}

void TabletServiceImpl::Ping(const PingRequestPB* req,
                             PingResponsePB* resp,
                             rpc::RpcContext* context) {
  context->RespondSuccess();
}

void TabletServiceImpl::SetupErrorAndRespond(TabletServerErrorPB* error,
                                             const Status &s,
                                             TabletServerErrorPB::Code code,
                                             rpc::RpcContext* context) const {
  StatusToPB(s, error->mutable_status());
  error->set_code(code);
  // TODO: rename RespondSuccess() to just "Respond" or
  // "SendResponse" since we use it for application-level error
  // responses, and this just looks confusing!
  context->RespondSuccess();
}

void TabletServiceImpl::Insert(const InsertRequestPB* req,
                               InsertResponsePB* resp,
                               rpc::RpcContext* context) {
  DVLOG(3) << "Received Insert RPC: " << req->DebugString();

  shared_ptr<Tablet> tablet;
  if (!server_->LookupTablet(req->tablet_id(), &tablet)) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::NotFound("Tablet not found"),
                         TabletServerErrorPB::TABLET_NOT_FOUND, context);
    return;
  }
  DCHECK(tablet) << "Null tablet";

  const RowwiseRowBlockPB& block_pb = req->data();

  // Check that the schema sent by the user matches the schema of the
  // tablet.
  Schema client_schema;
  Status s = ColumnPBsToSchema(block_pb.schema(), &client_schema);
  if (!s.ok()) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::INVALID_SCHEMA,
                         context);
    return;
  }

  if (!client_schema.Equals(tablet->schema())) {
    // TODO: support schema evolution.
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::InvalidArgument("Mismatched schema, expected",
                                                 tablet->schema().ToString()),
                         TabletServerErrorPB::MISMATCHED_SCHEMA,
                         context);
    return;
  }

  // In order to avoid a copy, ExtractRowsFromRowBlockPB mutates the
  // request RowBlock in-place. Because the RPC framework gives us our
  // request as a const argument, we have to const_cast it away here.
  // It's a little hacky, but the alternative of making all RPCs get
  // non-const requests doesn't seem that great either, since this
  // is a rare circumstance.
  RowwiseRowBlockPB* mutable_block_pb =
    const_cast<InsertRequestPB*>(req)->mutable_data();

  vector<const uint8_t*> to_insert;
  s = ExtractRowsFromRowBlockPB(client_schema, mutable_block_pb, &to_insert);
  if (!s.ok()) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::INVALID_ROW_BLOCK,
                         context);
    return;
  }


  int i = 0;
  BOOST_FOREACH(const uint8_t* row_ptr, to_insert) {
    ConstContiguousRow row(client_schema, row_ptr);
    DVLOG(2) << "Going to insert row: " << client_schema.DebugRow(row);

    Status s = tablet->Insert(row);
    if (PREDICT_FALSE(!s.ok())) {
      DVLOG(2) << "Error for row " << client_schema.DebugRow(row)
               << ": " << s.ToString();

      InsertResponsePB::PerRowErrorPB* error = resp->add_per_row_errors();
      error->set_row_index(i);
      StatusToPB(s, error->mutable_error());
    }
    i++;
  }

  context->RespondSuccess();
  return;
}

} // namespace tserver
} // namespace kudu
