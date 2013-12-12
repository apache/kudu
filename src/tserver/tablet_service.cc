// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tablet_service.h"

#include <algorithm>
#include <string>
#include <tr1/memory>
#include <vector>

#include "common/iterator.h"
#include "common/wire_protocol.h"
#include "consensus/consensus.h"
#include "gutil/casts.h"
#include "gutil/stl_util.h"
#include "rpc/rpc_context.h"
#include "tablet/tablet_peer.h"
#include "tablet/transactions/alter_schema_transaction.h"
#include "tablet/transactions/write_transaction.h"
#include "tserver/scanners.h"
#include "tserver/tablet_server.h"
#include "tserver/ts_tablet_manager.h"
#include "tserver/tserver.pb.h"
#include "util/monotime.h"
#include "util/status.h"
#include "util/trace.h"

using kudu::tablet::TabletPeer;
using kudu::tablet::AlterSchemaTransactionContext;
using kudu::tablet::WriteTransactionContext;
using kudu::tablet::TransactionCompletionCallback;
using std::tr1::shared_ptr;
using std::vector;
using google::protobuf::RepeatedPtrField;

DEFINE_int32(tablet_server_default_scan_batch_size_bytes, 1024 * 1024,
             "The default size for batches of scan results");
DEFINE_int32(tablet_server_max_scan_batch_size_bytes, 8 * 1024 * 1024,
             "The maximum batch size that a client may request for "
             "scan results.");
DEFINE_int32(tablet_server_scan_batch_size_rows, 100,
             "The number of rows to batch for servicing scan requests.");

namespace kudu {
namespace tserver {

static void SetupErrorAndRespond(TabletServerErrorPB* error,
                                 const Status& s,
                                 TabletServerErrorPB::Code code,
                                 rpc::RpcContext* context) {
  StatusToPB(s, error->mutable_status());
  error->set_code(code);
  // TODO: rename RespondSuccess() to just "Respond" or
  // "SendResponse" since we use it for application-level error
  // responses, and this just looks confusing!
  context->RespondSuccess();
}

static void RespondGenericError(const string& doing_what,
                                TabletServerErrorPB* error,
                                const Status& s,
                                rpc::RpcContext* context) {
  LOG(WARNING) << "Generic error " << doing_what << " for request "
               << context->request_pb()->ShortDebugString()
               << ": " << s.ToString();
  SetupErrorAndRespond(error, s, TabletServerErrorPB::UNKNOWN_ERROR, context);
}

// A transaction completion callback that responds to the client when transactions
// complete and sets the client error if there is one to set.
// TODO find a way to avoid passing specific responses (templating is worse as
// is pb reflection)
class RpcTransactionCompletionCallback : public TransactionCompletionCallback {
 public:
  RpcTransactionCompletionCallback(rpc::RpcContext* context,
                                   WriteResponsePB* w_resp)
 : context_(context),
   w_resp_(w_resp),
   as_resp_(NULL) {}

  RpcTransactionCompletionCallback(rpc::RpcContext* context,
                                   AlterSchemaResponsePB* as_resp)
  : context_(context),
    w_resp_(NULL),
    as_resp_(as_resp) {}

  virtual void TransactionCompleted() {
    if (!status_.ok()) {
      SetupErrorAndRespond(get_error(), status_, code_, context_);
    } else {
      context_->RespondSuccess();
    }
  };

 private:

  TabletServerErrorPB* get_error() {
    if (w_resp_)
      return w_resp_->mutable_error();
    return as_resp_->mutable_error();
  }

  rpc::RpcContext* context_;
  WriteResponsePB* w_resp_;
  AlterSchemaResponsePB* as_resp_;
};

TabletServiceImpl::TabletServiceImpl(TabletServer* server)
  : server_(server) {
}

void TabletServiceImpl::Ping(const PingRequestPB* req,
                             PingResponsePB* resp,
                             rpc::RpcContext* context) {
  context->RespondSuccess();
}

void TabletServiceImpl::AlterSchema(const AlterSchemaRequestPB* req,
                                    AlterSchemaResponsePB* resp,
                                    rpc::RpcContext* context) {
  DVLOG(3) << "Received Alter Schema RPC: " << req->DebugString();

  shared_ptr<TabletPeer> tablet_peer;
  if (!server_->tablet_manager()->LookupTablet(req->tablet_id(), &tablet_peer)) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::NotFound("Tablet not found"),
                         TabletServerErrorPB::TABLET_NOT_FOUND, context);
    return;
  }
  DCHECK(tablet_peer) << "Null tablet peer";

  AlterSchemaTransactionContext *tx_ctx =
    new AlterSchemaTransactionContext(tablet_peer.get(), req, resp);

  tx_ctx->set_completion_callback(gscoped_ptr<TransactionCompletionCallback>(
      new RpcTransactionCompletionCallback(context, resp)).Pass());

  // Submit the write. The RPC will be responded to asynchronously.
  Status s = tablet_peer->SubmitAlterSchema(tx_ctx);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
  }
}

void TabletServiceImpl::Write(const WriteRequestPB* req,
                              WriteResponsePB* resp,
                              rpc::RpcContext* context) {
  DVLOG(3) << "Received Write RPC: " << req->DebugString();

  shared_ptr<TabletPeer> tablet_peer;
  if (!server_->tablet_manager()->LookupTablet(req->tablet_id(), &tablet_peer)) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::NotFound("Tablet not found"),
                         TabletServerErrorPB::TABLET_NOT_FOUND, context);
    return;
  }
  DCHECK(tablet_peer) << "Null tablet peer";

  WriteTransactionContext *tx_ctx =
    new WriteTransactionContext(tablet_peer.get(), req, resp);

  tx_ctx->set_completion_callback(gscoped_ptr<TransactionCompletionCallback>(
      new RpcTransactionCompletionCallback(context, resp)).Pass());

  // Submit the write. The RPC will be responded to asynchronously.
  Status s = tablet_peer->SubmitWrite(tx_ctx);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
  }
}

void TabletServiceImpl::Scan(const ScanRequestPB* req,
                             ScanResponsePB* resp,
                             rpc::RpcContext* context) {
  // Validate the request: user must pass a new_scan_request or
  // a scanner ID, but not both.
  if (PREDICT_FALSE(req->has_scanner_id() &&
                    req->has_new_scan_request())) {
    context->RespondFailure(Status::InvalidArgument(
                              "Must not pass both a scanner_id and new_scan_request"));
    return;
  }

  if (req->has_new_scan_request()) {
    HandleNewScanRequest(req, resp, context);
  } else if (req->has_scanner_id()) {
    HandleContinueScanRequest(req, resp, context);
  } else {
    context->RespondFailure(Status::InvalidArgument(
                              "Must pass either a scanner_id or new_scan_request"));
  }
}

// Extract a void* pointer suitable for use in a ColumnRangePredicate from the
// user-specified protobuf field.
// This validates that the pb_value has the correct length, copies the data into
// 'pool', and sets *result to point to it.
// Returns bad status if the user-specified value is the wrong length.
static Status ExtractPredicateValue(const ColumnSchema& schema,
                                    const string& pb_value,
                                    AutoReleasePool* pool,
                                    const void** result) {
  // Copy the data from the protobuf into the pool.
  uint8_t* data_copy = pool->AddArray(new uint8_t[pb_value.size()]);
  memcpy(data_copy, &pb_value[0], pb_value.size());

  // If the type is a STRING, then we need to return a pointer to a Slice
  // element pointing to the string. Otherwise, just verify that the provided
  // value was the right size.
  if (schema.type_info().type() == STRING) {
    Slice* s = pool->Add(new Slice(data_copy, pb_value.size()));
    *result = s;
  } else {
    // TODO: add test case for this invalid request
    size_t expected_size = schema.type_info().size();
    if (pb_value.size() != expected_size) {
      return Status::InvalidArgument(
        StringPrintf("Bad predicate on %s. Expected value size %zd, got %zd",
                     schema.ToString().c_str(), expected_size, pb_value.size()));
    }
    *result = data_copy;
  }

  return Status::OK();
}

static Status SetupScanSpec(const NewScanRequestPB& scan_pb,
                            gscoped_ptr<ScanSpec>* spec,
                            AutoReleasePool* pool) {
  gscoped_ptr<ScanSpec> ret(new ScanSpec);
  BOOST_FOREACH(const ColumnRangePredicatePB& pred_pb, scan_pb.range_predicates()) {
    if (!pred_pb.has_lower_bound() && !pred_pb.has_upper_bound()) {
      return Status::InvalidArgument(
        string("Invalid predicate ") + pred_pb.ShortDebugString() +
        ": has no lower or upper bound.");
    }
    ColumnSchema col(ColumnSchemaFromPB(pred_pb.column()));

    boost::optional<const void*> lower_bound, upper_bound;
    if (pred_pb.has_lower_bound()) {
      const void* val;
      RETURN_NOT_OK(ExtractPredicateValue(col, pred_pb.lower_bound(), pool,
                                          &val));
      lower_bound = val;
    }
    if (pred_pb.has_upper_bound()) {
      const void* val;
      RETURN_NOT_OK(ExtractPredicateValue(col, pred_pb.upper_bound(), pool,
                                          &val));
      upper_bound = val;
    }

    ColumnRangePredicate pred(col, lower_bound, upper_bound);
    if (VLOG_IS_ON(3)) {
      VLOG(3) << "Parsed predicate " << pred.ToString() << " from " << scan_pb.ShortDebugString();
    }
    ret->AddPredicate(pred);
  }
  spec->swap(ret);
  return Status::OK();
}

// Start a new scan.
void TabletServiceImpl::HandleNewScanRequest(const ScanRequestPB* req,
                                             ScanResponsePB* resp,
                                             rpc::RpcContext* context) {
  DCHECK(req->has_new_scan_request());

  const NewScanRequestPB& scan_pb = req->new_scan_request();
  shared_ptr<TabletPeer> tablet_peer;
  if (PREDICT_FALSE(!server_->tablet_manager()->LookupTablet(
                      scan_pb.tablet_id(), &tablet_peer))) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::NotFound("Tablet not found"),
                         TabletServerErrorPB::TABLET_NOT_FOUND, context);
    return;
  }

  // Create the user's requested projection.
  // TODO: add test cases for bad projections including 0 columns
  Schema projection;
  Status s = ColumnPBsToSchema(scan_pb.projected_columns(), &projection);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::INVALID_SCHEMA,
                         context);
    return;
  }

  if (projection.has_column_ids()) {
    s = Status::InvalidArgument("User requests should not have Column IDs");
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::INVALID_SCHEMA,
                         context);
    return;
  }

  if (projection.has_column_ids()) {
    s = Status::InvalidArgument("User requests should not have Column IDs");
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::INVALID_SCHEMA,
                         context);
    return;
  }

  AutoReleasePool pool;
  gscoped_ptr<ScanSpec> spec(new ScanSpec);
  s = SetupScanSpec(scan_pb, &spec, &pool);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::INVALID_SCAN_SPEC,
                         context);
    return;
  }

  context->trace()->Message("Creating iterator");
  gscoped_ptr<RowwiseIterator> iter;
  s = tablet_peer->tablet()->NewRowIterator(projection, &iter);
  if (s.ok()) {
    s = iter->Init(spec.get());
  }
  context->trace()->Message("Iterator initialized");

  if (PREDICT_FALSE(s.IsInvalidArgument())) {
    // An invalid projection returns InvalidArgument above.
    // TODO: would be nice if we threaded these more specific
    // error codes throughout Kudu.
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::MISMATCHED_SCHEMA,
                         context);
    return;
  } else if (PREDICT_FALSE(!s.ok())) {
    RespondGenericError("setting up scanner", resp->mutable_error(), s, context);
    return;
  }

  bool has_more = iter->HasNext();
  resp->set_has_more_results(has_more);
  if (!has_more) {
    // If there are no more rows, there is no need to assign a scanner ID.
    // Just respond immediately instead.
    context->RespondSuccess();
    return;
  }

  SharedScanner scanner;
  server_->scanner_manager()->NewScanner(&scanner);
  scanner->Init(iter.Pass());

  // The ScanSpec has to remain valid as long as the scanner, so move its
  // ownership into the scanner itself.
  scanner->autorelease_pool()->Add(spec.release());
  pool.DonateAllTo(scanner->autorelease_pool());

  // TODO: could start the scan here unless batch_size_bytes is 0
  resp->set_scanner_id(scanner->id());

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Started scanner " << scanner->id() << ": " << scanner->iter()->ToString();
  }

  context->RespondSuccess();
}

// Return the batch size to use for a given request, after clamping
// the user-requested request within the server-side allowable range.
static size_t GetBatchSizeBytes(const ScanRequestPB* req) {
  if (!req->has_batch_size_bytes()) {
    return FLAGS_tablet_server_default_scan_batch_size_bytes;
  }

  return std::min(req->batch_size_bytes(),
                  implicit_cast<uint32_t>(FLAGS_tablet_server_max_scan_batch_size_bytes));
}

// Continue an existing scan request.
void TabletServiceImpl::HandleContinueScanRequest(const ScanRequestPB* req,
                                                  ScanResponsePB* resp,
                                                  rpc::RpcContext* context) {
  DCHECK(req->has_scanner_id());

  // TODO: need some kind of concurrency control on these scanner objects
  // in case multiple RPCs hit the same scanner at the same time. Probably
  // just a trylock and fail the RPC if it contends.
  SharedScanner scanner;
  if (!server_->scanner_manager()->LookupScanner(req->scanner_id(), &scanner)) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::NotFound("Scanner not found"),
                         TabletServerErrorPB::SCANNER_EXPIRED, context);
    return;
  }
  VLOG(2) << "Found existing scanner " << scanner->id() << " for request: "
          << req->ShortDebugString();
  context->trace()->SubstituteAndTrace("Found scanner $0", scanner->id());

  // TODO: check the call_seq_id!

  scanner->UpdateAccessTime();

  RowwiseIterator* iter = scanner->iter();

  // TODO: could size the RowBlock based on the user's requested batch size?
  // If people had really large indirect objects, we would currently overshoot
  // their requested batch size by a lot.
  Arena arena(32 * 1024, 1 * 1024 * 1024);
  RowBlock block(scanner->iter()->schema(),
                 FLAGS_tablet_server_scan_batch_size_rows, &arena);

  size_t batch_size_bytes = GetBatchSizeBytes(req);
  resp->mutable_data()->mutable_rows()->reserve(batch_size_bytes * 11 / 10);

  // TODO: in the future, use the client timeout to set a budget. For now,
  // just use a half second, which should be plenty to amortize call overhead.
  int budget_ms = 500;
  MonoTime deadline = MonoTime::Now(MonoTime::COARSE);
  deadline.AddDelta(MonoDelta::FromMilliseconds(budget_ms));

  while (iter->HasNext()) {
    Status s = RowwiseIterator::CopyBlock(iter, &block);
    if (PREDICT_FALSE(!s.ok())) {
      RespondGenericError("copying rows from internal iterator",
                          resp->mutable_error(), s, context);
      return;
    }

    ConvertRowBlockToPB(block, resp->mutable_data());

    // TODO: could break if it's been looping too long - eg with restrictive predicates,
    // we don't want to loop here for too long monopolizing a thread and risking a
    // client timeout.
    //
    // TODO: should check if RPC got cancelled, once we implement RPC cancellation.
    size_t response_size = resp->data().rows().size() + resp->data().indirect_data().size();
    context->trace()->SubstituteAndTrace("Copied block, new size=$0", response_size);

    MonoTime now = MonoTime::Now(MonoTime::COARSE);
    if (!now.ComesBefore(deadline)) {
      context->trace()->Message("Deadline expired - responding early");
      break;
    }

    if (response_size >= batch_size_bytes) {
      break;
    }
  }

  scanner->UpdateAccessTime();
  bool has_more = iter->HasNext();
  resp->set_has_more_results(has_more);
  if (!has_more) {
    VLOG(2) << "Scanner " << scanner->id() << " complete: removing...";
    bool success = server_->scanner_manager()->UnregisterScanner(req->scanner_id());
    LOG_IF(WARNING, !success) << "Scanner " << scanner->id() <<
      " not removed successfully from scanner manager. May be a bug.";
  }

  context->RespondSuccess();
}

} // namespace tserver
} // namespace kudu
