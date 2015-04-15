// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tserver/tablet_service.h"

#include <algorithm>
#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/common/iterator.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tserver/remote_bootstrap_service.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tablet/transactions/change_config_transaction.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

DEFINE_int32(tablet_server_default_scan_batch_size_bytes, 1024 * 1024,
             "The default size for batches of scan results");
DEFINE_int32(tablet_server_max_scan_batch_size_bytes, 8 * 1024 * 1024,
             "The maximum batch size that a client may request for "
             "scan results.");
DEFINE_int32(tablet_server_scan_batch_size_rows, 100,
             "The number of rows to batch for servicing scan requests.");

namespace kudu {
namespace tserver {

using consensus::ConsensusRequestPB;
using consensus::ConsensusResponsePB;
using consensus::ChangeConfigRequestPB;
using consensus::ChangeConfigResponsePB;
using consensus::GetNodeInstanceRequestPB;
using consensus::GetNodeInstanceResponsePB;
using consensus::RunLeaderElectionRequestPB;
using consensus::RunLeaderElectionResponsePB;
using consensus::VoteRequestPB;
using consensus::VoteResponsePB;

using google::protobuf::RepeatedPtrField;
using rpc::RpcContext;
using std::tr1::shared_ptr;
using std::vector;
using tablet::AlterSchemaTransactionState;
using tablet::ChangeConfigTransactionState;
using tablet::TabletPeer;
using tablet::TabletStatusPB;
using tablet::TransactionCompletionCallback;
using tablet::WriteTransactionState;

namespace {

// Lookup the given tablet, ensuring that it both exists and is RUNNING.
// If it is not, responds to the RPC associated with 'context' after setting
// resp->mutable_error() to indicate the failure reason.
//
// Returns true if successful.
template<class RespClass>
bool LookupTabletOrRespond(TabletPeerLookupIf* tablet_manager,
                           const string& tablet_id,
                           RespClass* resp,
                           rpc::RpcContext* context,
                           scoped_refptr<TabletPeer>* peer) {
  if (PREDICT_FALSE(!tablet_manager->GetTabletPeer(tablet_id, peer).ok())) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::NotFound("Tablet not found"),
                         TabletServerErrorPB::TABLET_NOT_FOUND, context);
    return false;
  }

  // Check RUNNING state.
  tablet::TabletStatePB state = (*peer)->state();
  if (PREDICT_FALSE(state != tablet::RUNNING)) {
    Status s = Status::IllegalState("Tablet not RUNNING",
                                    tablet::TabletStatePB_Name(state));
    if (state == tablet::FAILED) {
      s = s.CloneAndAppend((*peer)->error().ToString());
    }
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::TABLET_NOT_RUNNING, context);
    return false;
  }
  return true;
}
} // namespace

typedef ListTabletsResponsePB::StatusAndSchemaPB StatusAndSchemaPB;

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
template<class Response>
class RpcTransactionCompletionCallback : public TransactionCompletionCallback {
 public:
  RpcTransactionCompletionCallback(rpc::RpcContext* context,
                                   Response* response)
 : context_(context),
   response_(response) {}

  virtual void TransactionCompleted() OVERRIDE {
    if (!status_.ok()) {
      SetupErrorAndRespond(get_error(), status_, code_, context_);
    } else {
      context_->RespondSuccess();
    }
  };

 private:

  TabletServerErrorPB* get_error() {
    return response_->mutable_error();
  }

  rpc::RpcContext* context_;
  Response* response_;
  tablet::TransactionState* state_;
};

// Generic interface to handle scan results.
class ScanResultCollector {
 public:
  virtual void HandleRowBlock(const Schema* client_projection_schema,
                              const RowBlock& row_block) = 0;

  // Returns number of times HandleRowBlock() was called.
  virtual int BlocksProcessed() const = 0;

  // Returns number of bytes processed.
  virtual int64_t BytesRead() const = 0;

  // Returns the encoded last row processed.
  virtual const faststring& encoded_last_row() const = 0;
};

namespace {

// Given a RowBlock, set encoded_last_row to the encoded key of the last selected row
// in the RowBlock. If no row is selected, encoded_last_row is not set.
void SetLastRow(const RowBlock& row_block, faststring* encoded_last_row) {
  // Find the last selected row and save its encoded key.
  const SelectionVector* sel = row_block.selection_vector();
  if (sel->AnySelected()) {
    for (int i = sel->nrows() - 1; i >= 0; i--) {
      if (sel->IsRowSelected(i)) {
        RowBlockRow last_row = row_block.row(i);
        const Schema* schema = last_row.schema();
        schema->EncodeComparableKey(last_row, encoded_last_row);
        break;
      }
    }
  }
}

}  // namespace

// Copies the scan result to the given row block PB and data buffers.
//
// This implementation is used in the common case where a client is running
// a scan and the data needs to be returned to the client.
//
// (This is in contrast to some other ScanResultCollector implementation that
// might do an aggregation or gather some other types of statistics via a
// server-side scan and thus never need to return the actual data.)
class ScanResultCopier : public ScanResultCollector {
 public:
  ScanResultCopier(RowwiseRowBlockPB* rowblock_pb, faststring* rows_data, faststring* indirect_data)
      : rowblock_pb_(DCHECK_NOTNULL(rowblock_pb)),
        rows_data_(DCHECK_NOTNULL(rows_data)),
        indirect_data_(DCHECK_NOTNULL(indirect_data)),
        blocks_processed_(0) {
  }

  virtual void HandleRowBlock(const Schema* client_projection_schema,
                              const RowBlock& row_block) OVERRIDE {
    blocks_processed_++;
    SerializeRowBlock(row_block, rowblock_pb_, client_projection_schema,
                      rows_data_, indirect_data_);
    SetLastRow(row_block, &encoded_last_row_);
  }

  virtual int BlocksProcessed() const OVERRIDE { return blocks_processed_; }

  // Returns number of bytes buffered to return.
  virtual int64_t BytesRead() const OVERRIDE {
    return rows_data_->size() + indirect_data_->size();
  }

  virtual const faststring& encoded_last_row() const OVERRIDE {
    return encoded_last_row_;
  }

 private:
  RowwiseRowBlockPB* const rowblock_pb_;
  faststring* const rows_data_;
  faststring* const indirect_data_;
  int blocks_processed_;
  faststring encoded_last_row_;

  DISALLOW_COPY_AND_ASSIGN(ScanResultCopier);
};

// Calculates a CRC32C for the given row.
// Also returns number of bytes read in out param.
static uint32_t CalcRowCrc32(const Schema& projection, const RowBlockRow& row,
                             crc::Crc* crc, size_t* bytes_read) {
  *bytes_read = 0;
  uint64_t row_crc = 0;
  for (size_t j = 0; j < projection.num_columns(); j++) {
    uint32_t col_index = static_cast<uint32_t>(j);  // For the CRC.
    crc->Compute(&col_index, sizeof(col_index), &row_crc, NULL);
    ColumnBlockCell cell = row.cell(j);
    if (cell.is_nullable()) {
      uint8_t is_defined = cell.is_null() ? 0 : 1;
      crc->Compute(&is_defined, sizeof(is_defined), &row_crc, NULL);
      if (!is_defined) continue;
    }
    if (cell.type() == STRING) {
      const Slice* data = reinterpret_cast<const Slice *>(cell.ptr());
      crc->Compute(data->data(), data->size(), &row_crc, NULL);
      *bytes_read += data->size();
    } else {
      crc->Compute(cell.ptr(), cell.size(), &row_crc, NULL);
      *bytes_read += cell.size();
    }
  }

  return static_cast<uint32_t>(row_crc); // CRC32 only uses the lower 32 bits.
}

// Checksums the scan result.
class ScanResultChecksummer : public ScanResultCollector {
 public:
  ScanResultChecksummer()
      : crc_(crc::GetCrc32cInstance()),
        agg_checksum_(0),
        bytes_read_(0),
        blocks_processed_(0) {
  }

  virtual void HandleRowBlock(const Schema* client_projection_schema,
                              const RowBlock& row_block) OVERRIDE {
    blocks_processed_++;
    if (!client_projection_schema) {
      client_projection_schema = &row_block.schema();
    }

    size_t nrows = row_block.nrows();
    for (size_t i = 0; i < nrows; i++) {
      if (!row_block.selection_vector()->IsRowSelected(i)) continue;
      size_t bytes_read;
      uint32_t row_crc = CalcRowCrc32(*client_projection_schema, row_block.row(i), crc_,
                                      &bytes_read);
      agg_checksum_ += row_crc;
      bytes_read_ += bytes_read;
    }
    // Find the last selected row and save its encoded key.
    SetLastRow(row_block, &encoded_last_row_);
  }

  virtual int BlocksProcessed() const OVERRIDE { return blocks_processed_; }

  // Returns number of bytes scanned.
  virtual int64_t BytesRead() const OVERRIDE { return bytes_read_; }

  virtual const faststring& encoded_last_row() const OVERRIDE { return encoded_last_row_; }

  // Accessors for initializing / setting the checksum.
  void set_agg_checksum(uint64_t value) { agg_checksum_ = value; }
  uint64_t agg_checksum() const { return agg_checksum_; }

 private:
  crc::Crc* const crc_;
  uint64_t agg_checksum_;
  int64_t bytes_read_;
  int blocks_processed_;
  faststring encoded_last_row_;

  DISALLOW_COPY_AND_ASSIGN(ScanResultChecksummer);
};

// Return the batch size to use for a given request, after clamping
// the user-requested request within the server-side allowable range.
// This is only a hint, really more of a threshold since returned bytes
// may exceed this limit, but hopefully only by a little bit.
static size_t GetMaxBatchSizeBytesHint(const ScanRequestPB* req) {
  if (!req->has_batch_size_bytes()) {
    return FLAGS_tablet_server_default_scan_batch_size_bytes;
  }

  return std::min(req->batch_size_bytes(),
                  implicit_cast<uint32_t>(FLAGS_tablet_server_max_scan_batch_size_bytes));
}

TabletServiceImpl::TabletServiceImpl(TabletServer* server)
  : TabletServerServiceIf(server->metric_entity()),
    server_(server) {
}

void TabletServiceImpl::Ping(const PingRequestPB* req,
                             PingResponsePB* resp,
                             rpc::RpcContext* context) {
  context->RespondSuccess();
}

TabletServiceAdminImpl::TabletServiceAdminImpl(TabletServer* server)
  : TabletServerAdminServiceIf(server->metric_entity()),
    server_(server) {
}

void TabletServiceAdminImpl::AlterSchema(const AlterSchemaRequestPB* req,
                                         AlterSchemaResponsePB* resp,
                                         rpc::RpcContext* context) {
  DVLOG(3) << "Received Alter Schema RPC: " << req->DebugString();

  scoped_refptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(server_->tablet_manager(), req->tablet_id(), resp, context,
                             &tablet_peer)) {
    return;
  }

  uint32_t schema_version = tablet_peer->tablet()->metadata()->schema_version();

  // If the schema was already applied, respond as succeded
  if (schema_version == req->schema_version()) {
    // Sanity check, to verify that the tablet should have the same schema
    // specified in the request.
    Schema req_schema;
    Status s = SchemaFromPB(req->schema(), &req_schema);
    if (!s.ok()) {
      SetupErrorAndRespond(resp->mutable_error(), s,
                           TabletServerErrorPB::INVALID_SCHEMA, context);
      return;
    }

    Schema tablet_schema = tablet_peer->tablet()->metadata()->schema();
    if (req_schema.Equals(tablet_schema)) {
      context->RespondSuccess();
      return;
    }

    schema_version = tablet_peer->tablet()->metadata()->schema_version();
    if (schema_version == req->schema_version()) {
      LOG(ERROR) << "The current schema does not match the request schema."
                 << " version=" << schema_version
                 << " current-schema=" << tablet_schema.ToString()
                 << " request-schema=" << req_schema.ToString()
                 << " (corruption)";
      SetupErrorAndRespond(resp->mutable_error(),
                           Status::Corruption("got a different schema for the same version number"),
                           TabletServerErrorPB::MISMATCHED_SCHEMA, context);
      return;
    }
  }

  // If the current schema is newer than the one in the request reject the request.
  if (schema_version > req->schema_version()) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::InvalidArgument("Tablet has a newer schema"),
                         TabletServerErrorPB::TABLET_HAS_A_NEWER_SCHEMA, context);
    return;
  }

  AlterSchemaTransactionState *tx_state =
    new AlterSchemaTransactionState(tablet_peer.get(), req, resp);

  tx_state->set_completion_callback(gscoped_ptr<TransactionCompletionCallback>(
      new RpcTransactionCompletionCallback<AlterSchemaResponsePB>(context,
                                                                  resp)).Pass());

  // Submit the alter schema op. The RPC will be responded to asynchronously.
  Status s = tablet_peer->SubmitAlterSchema(tx_state);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
}

void TabletServiceAdminImpl::CreateTablet(const CreateTabletRequestPB* req,
                                          CreateTabletResponsePB* resp,
                                          rpc::RpcContext* context) {
  TRACE_EVENT1("tserver", "CreateTablet",
               "tablet_id", req->tablet_id());
  LOG(INFO) << "Processing CreateTablet for tablet " << req->tablet_id()
            << " (table=" << req->table_name()
            << " [id=" << req->table_id() << "]), range=[\""
            << strings::CHexEscape(req->start_key()) << "\", \""
            << strings::CHexEscape(req->end_key()) << "\"]";
  VLOG(1) << "Full request: " << req->DebugString();

  Schema schema;
  Status s = SchemaFromPB(req->schema(), &schema);
  DCHECK(schema.has_column_ids());
  if (!s.ok()) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::IllegalState("Invalid Schema."),
                         TabletServerErrorPB::INVALID_SCHEMA, context);
    return;
  }

  s = server_->tablet_manager()->CreateNewTablet(req->table_id(),
                                                 req->tablet_id(),
                                                 req->start_key(),
                                                 req->end_key(),
                                                 req->table_name(),
                                                 schema,
                                                 req->quorum(),
                                                 NULL);
  if (PREDICT_FALSE(!s.ok())) {
    TabletServerErrorPB::Code code;
    if (s.IsAlreadyPresent()) {
      code = TabletServerErrorPB::TABLET_ALREADY_EXISTS;
    } else {
      code = TabletServerErrorPB::UNKNOWN_ERROR;
    }
    SetupErrorAndRespond(resp->mutable_error(), s, code, context);
    return;
  }
  context->RespondSuccess();
}

void TabletServiceAdminImpl::DeleteTablet(const DeleteTabletRequestPB* req,
                                          DeleteTabletResponsePB* resp,
                                          rpc::RpcContext* context) {
  LOG(INFO) << "Processing DeleteTablet for tablet " << req->tablet_id()
            << (req->has_reason() ? (" (" + req->reason() + ")") : "")
            << " from " << context->requestor_string();
  VLOG(1) << "Full request: " << req->DebugString();

  scoped_refptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(server_->tablet_manager(), req->tablet_id(), resp, context,
                             &tablet_peer)) {
    return;
  }

  Status s = server_->tablet_manager()->DeleteTablet(tablet_peer);
  if (PREDICT_FALSE(!s.ok())) {
    TabletServerErrorPB::Code code;
    if (s.IsNotFound()) {
      code = TabletServerErrorPB::TABLET_NOT_FOUND;
    } else if (s.IsServiceUnavailable()) {
      code = TabletServerErrorPB::TABLET_NOT_RUNNING;
    } else {
      code = TabletServerErrorPB::UNKNOWN_ERROR;
    }
    SetupErrorAndRespond(resp->mutable_error(), s, code, context);
    return;
  }
  context->RespondSuccess();
}

void TabletServiceImpl::Write(const WriteRequestPB* req,
                              WriteResponsePB* resp,
                              rpc::RpcContext* context) {
  TRACE_EVENT1("tserver", "TabletServiceImpl::Write",
               "tablet_id", req->tablet_id());
  DVLOG(3) << "Received Write RPC: " << req->DebugString();

  scoped_refptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(server_->tablet_manager(), req->tablet_id(), resp, context,
                             &tablet_peer)) {
    return;
  }

  if (req->external_consistency_mode() != NO_CONSISTENCY) {
    if (!server_->clock()->SupportsExternalConsistencyMode(req->external_consistency_mode())) {
      Status s = Status::ServiceUnavailable("The configured clock does not support the"
          " required consistency mode.");
      SetupErrorAndRespond(resp->mutable_error(), s,
                           TabletServerErrorPB::UNKNOWN_ERROR,
                           context);
      return;
    }
  }

  WriteTransactionState *tx_state =
    new WriteTransactionState(tablet_peer.get(), req, resp);

  // If the consistency mode is set to CLIENT_PROPAGATED and the client
  // sent us a timestamp, decode it and set it in the transaction context.
  // Also update the clock so that all future timestamps are greater than
  // the passed timestamp.
  if (req->external_consistency_mode() == CLIENT_PROPAGATED) {
    Status s;
    if (req->has_propagated_timestamp()) {
      Timestamp ts(req->propagated_timestamp());
      if (PREDICT_TRUE(s.ok())) {
        tx_state->set_client_propagated_timestamp(ts);
        // update the clock with the client's timestamp
        s = server_->clock()->Update(ts);
      }
    }
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(), s,
                           TabletServerErrorPB::UNKNOWN_ERROR,
                           context);
      return;
    }
  }

  tx_state->set_completion_callback(gscoped_ptr<TransactionCompletionCallback>(
      new RpcTransactionCompletionCallback<WriteResponsePB>(context,
                                                            resp)).Pass());

  // Submit the write. The RPC will be responded to asynchronously.
  Status s = tablet_peer->SubmitWrite(tx_state);

  // Check that we could submit the write
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                               TabletServerErrorPB::UNKNOWN_ERROR,
                               context);
  }
  return;
}

ConsensusServiceImpl::ConsensusServiceImpl(const scoped_refptr<MetricEntity>& metric_entity,
                                           TabletPeerLookupIf* tablet_manager)
  : ConsensusServiceIf(metric_entity),
    tablet_manager_(tablet_manager) {
}

ConsensusServiceImpl::~ConsensusServiceImpl() {
}

void ConsensusServiceImpl::ChangeConfig(const consensus::ChangeConfigRequestPB* req,
                                        ChangeConfigResponsePB* resp,
                                        rpc::RpcContext* context) {
  DVLOG(3) << "Received Change Config RPC: " << req->DebugString();

  scoped_refptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(tablet_manager_, req->tablet_id(), resp, context,
                             &tablet_peer)) {
    return;
  }

  ChangeConfigTransactionState *tx_state =
    new ChangeConfigTransactionState(tablet_peer.get(), req, resp);

  tx_state->set_completion_callback(gscoped_ptr<TransactionCompletionCallback>(
      new RpcTransactionCompletionCallback<ChangeConfigResponsePB>(context,
                                                                   resp)).Pass());

  // Submit the change config op. The RPC will be responded to asynchronously.
  Status s = tablet_peer->SubmitChangeConfig(tx_state);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
}


void ConsensusServiceImpl::UpdateConsensus(const ConsensusRequestPB* req,
                                           ConsensusResponsePB* resp,
                                           rpc::RpcContext* context) {
  DVLOG(3) << "Received Consensus Update RPC: " << req->DebugString();
  scoped_refptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(tablet_manager_, req->tablet_id(), resp, context, &tablet_peer)) {
    return;
  }

  // Submit the update directly to the TabletPeer's Consensus instance.
  Status s = tablet_peer->consensus()->Update(req, resp);
  if (PREDICT_FALSE(!s.ok())) {
    // Clear the response first, since a partially-filled response could
    // result in confusing a caller, or in having missing required fields
    // in embedded optional messages.
    resp->Clear();

    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::RequestConsensusVote(const VoteRequestPB* req,
                                                VoteResponsePB* resp,
                                                rpc::RpcContext* context) {
  DVLOG(3) << "Received Consensus Request Vote RPC: " << req->DebugString();
  scoped_refptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(tablet_manager_, req->tablet_id(), resp, context, &tablet_peer)) {
    return;
  }

  // Submit the vote request directly to the consensus instance.
  Status s = tablet_peer->consensus()->RequestVote(req, resp);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::GetNodeInstance(const GetNodeInstanceRequestPB* req,
                                           GetNodeInstanceResponsePB* resp,
                                           rpc::RpcContext* context) {
  DVLOG(3) << "Received Get Node Instance RPC: " << req->DebugString();
  resp->mutable_node_instance()->CopyFrom(tablet_manager_->NodeInstance());
  context->RespondSuccess();
}

void ConsensusServiceImpl::RunLeaderElection(const RunLeaderElectionRequestPB* req,
                                             RunLeaderElectionResponsePB* resp,
                                             rpc::RpcContext* context) {
  DVLOG(3) << "Received Run Leader Election RPC: " << req->DebugString();
  scoped_refptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(tablet_manager_, req->tablet_id(), resp, context, &tablet_peer)) {
    return;
  }

  Status s = tablet_peer->consensus()->StartElection();
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::GetLastOpId(const consensus::GetLastOpIdRequestPB *req,
                                       consensus::GetLastOpIdResponsePB *resp,
                                       rpc::RpcContext *context) {
  DVLOG(3) << "Received GetLastOpId RPC: " << req->DebugString();
  scoped_refptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(tablet_manager_, req->tablet_id(), resp, context, &tablet_peer)) {
    return;
  }

  if (tablet_peer->state() != tablet::RUNNING) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::ServiceUnavailable("Tablet Peer not in RUNNING state"),
                         TabletServerErrorPB::TABLET_NOT_RUNNING, context);
    return;
  }
  Status s = tablet_peer->consensus()->GetLastReceivedOpId(resp->mutable_opid());
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::GetCommittedQuorum(const consensus::GetCommittedQuorumRequestPB *req,
                                              consensus::GetCommittedQuorumResponsePB *resp,
                                              rpc::RpcContext *context) {
  DVLOG(3) << "Received GetCommittedQuorum RPC: " << req->DebugString();
  scoped_refptr<TabletPeer> tablet_peer;
  if (!LookupTabletOrRespond(tablet_manager_, req->tablet_id(), resp, context, &tablet_peer)) {
    return;
  }

  resp->mutable_quorum()->CopyFrom(tablet_peer->consensus()->Quorum());
  context->RespondSuccess();
}


void TabletServiceImpl::Scan(const ScanRequestPB* req,
                             ScanResponsePB* resp,
                             rpc::RpcContext* context) {
  TRACE_EVENT0("tserver", "TabletServiceImpl::Scan");
  // Validate the request: user must pass a new_scan_request or
  // a scanner ID, but not both.
  if (PREDICT_FALSE(req->has_scanner_id() &&
                    req->has_new_scan_request())) {
    context->RespondFailure(Status::InvalidArgument(
                            "Must not pass both a scanner_id and new_scan_request"));
    return;
  }

  size_t batch_size_bytes = GetMaxBatchSizeBytesHint(req);
  gscoped_ptr<faststring> rows_data(new faststring(batch_size_bytes * 11 / 10));
  gscoped_ptr<faststring> indirect_data(new faststring());
  RowwiseRowBlockPB data;
  ScanResultCopier collector(&data, rows_data.get(), indirect_data.get());

  bool has_more_results = false;
  TabletServerErrorPB::Code error_code;
  if (req->has_new_scan_request()) {
    const NewScanRequestPB& scan_pb = req->new_scan_request();
    scoped_refptr<TabletPeer> tablet_peer;
    if (!LookupTabletOrRespond(server_->tablet_manager(), scan_pb.tablet_id(), resp, context,
                              &tablet_peer)) {
      return;
    }
    string scanner_id;
    Timestamp scan_timestamp;
    Status s = HandleNewScanRequest(tablet_peer.get(), req, context->requestor_string(),
                                    &collector, &scanner_id, &scan_timestamp, &has_more_results,
                                    &error_code);
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
      return;
    }
    resp->set_scanner_id(scanner_id);
    if (scan_timestamp != Timestamp::kInvalidTimestamp) {
      resp->set_snap_timestamp(scan_timestamp.ToUint64());
    }
  } else if (req->has_scanner_id()) {
    Status s = HandleContinueScanRequest(req, &collector, &has_more_results, &error_code);
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
      return;
    }
  } else {
    context->RespondFailure(Status::InvalidArgument(
                              "Must pass either a scanner_id or new_scan_request"));
    return;
  }
  resp->set_has_more_results(has_more_results);

  DVLOG(2) << "Blocks processed: " << collector.BlocksProcessed();
  if (collector.BlocksProcessed() > 0) {
    resp->mutable_data()->CopyFrom(data);

    // Add sidecar data to context and record the returned indices.
    int rows_idx;
    Status s = context->AddRpcSidecar(make_gscoped_ptr(
        new rpc::RpcSidecar(rows_data.Pass())), &rows_idx);
    if (!s.ok()) {
      RespondGenericError("Scan request main row data issue", resp->mutable_error(), s, context);
      return;
    }
    resp->mutable_data()->set_rows_sidecar(rows_idx);

    // Add indirect data as a sidecar, if applicable.
    if (indirect_data->size() > 0) {
      int indirect_idx;
      s = context->AddRpcSidecar(make_gscoped_ptr(
          new rpc::RpcSidecar(indirect_data.Pass())), &indirect_idx);
      if (!s.ok()) {
        RespondGenericError("Scan request indirect data issue", resp->mutable_error(), s, context);
        return;
      }
      resp->mutable_data()->set_indirect_data_sidecar(indirect_idx);
    }

    // Set the last row found by the collector.
    // We could have an empty batch if all the remaining rows are filtered by the predicate,
    // in which case do not set the last row.
    const faststring& last = collector.encoded_last_row();
    if (last.length() > 0) {
      resp->set_encoded_last_row_key(last.ToString());
    }
  }

  context->RespondSuccess();
}

void TabletServiceImpl::ListTablets(const ListTabletsRequestPB* req,
                                    ListTabletsResponsePB* resp,
                                    rpc::RpcContext* context) {
  vector<scoped_refptr<TabletPeer> > peers;
  server_->tablet_manager()->GetTabletPeers(&peers);
  RepeatedPtrField<StatusAndSchemaPB>* peer_status = resp->mutable_status_and_schema();
  BOOST_FOREACH(const scoped_refptr<TabletPeer>& peer, peers) {
    StatusAndSchemaPB* status = peer_status->Add();
    peer->GetTabletStatusPB(status->mutable_tablet_status());
    CHECK_OK(SchemaToPB(peer->status_listener()->schema(),
                        status->mutable_schema()));
  }
  context->RespondSuccess();
}

void TabletServiceImpl::Checksum(const ChecksumRequestPB* req,
                                 ChecksumResponsePB* resp,
                                 rpc::RpcContext* context) {
  VLOG(1) << "Full request: " << req->DebugString();

  // Validate the request: user must pass a new_scan_request or
  // a scanner ID, but not both.
  if (PREDICT_FALSE(req->has_new_request() &&
                    req->has_continue_request())) {
    context->RespondFailure(Status::InvalidArgument(
                            "Must not pass both a scanner_id and new_scan_request"));
    return;
  }

  // Convert ChecksumRequestPB to a ScanRequestPB.
  ScanRequestPB scan_req;
  if (req->has_call_seq_id()) scan_req.set_call_seq_id(req->call_seq_id());
  if (req->has_batch_size_bytes()) scan_req.set_batch_size_bytes(req->batch_size_bytes());
  if (req->has_close_scanner()) scan_req.set_close_scanner(req->close_scanner());

  ScanResultChecksummer collector;
  bool has_more = false;
  TabletServerErrorPB::Code error_code;
  if (req->has_new_request()) {
    scan_req.mutable_new_scan_request()->CopyFrom(req->new_request());
    const NewScanRequestPB& new_req = req->new_request();
    scoped_refptr<TabletPeer> tablet_peer;
    if (!LookupTabletOrRespond(server_->tablet_manager(), new_req.tablet_id(), resp, context,
                              &tablet_peer)) {
      return;
    }

    string scanner_id;
    Timestamp snap_timestamp;
    Status s = HandleNewScanRequest(tablet_peer.get(), &scan_req, context->requestor_string(),
                                    &collector, &scanner_id, &snap_timestamp, &has_more,
                                    &error_code);
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
      return;
    }
    resp->set_scanner_id(scanner_id);
    if (snap_timestamp != Timestamp::kInvalidTimestamp) {
      resp->set_snap_timestamp(snap_timestamp.ToUint64());
    }
  } else if (req->has_continue_request()) {
    const ContinueChecksumRequestPB& continue_req = req->continue_request();
    collector.set_agg_checksum(continue_req.previous_checksum());
    scan_req.set_scanner_id(continue_req.scanner_id());
    Status s = HandleContinueScanRequest(&scan_req, &collector, &has_more, &error_code);
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
      return;
    }
  } else {
    context->RespondFailure(Status::InvalidArgument(
                            "Must pass either new_request or continue_request"));
    return;
  }

  resp->set_checksum(collector.agg_checksum());
  resp->set_has_more_results(has_more);

  context->RespondSuccess();
}

void TabletServiceImpl::Shutdown() {
}

// Extract a void* pointer suitable for use in a ColumnRangePredicate from the
// user-specified protobuf field.
// This validates that the pb_value has the correct length, copies the data into
// 'arena', and sets *result to point to it.
// Returns bad status if the user-specified value is the wrong length.
static Status ExtractPredicateValue(const ColumnSchema& schema,
                                    const string& pb_value,
                                    Arena* arena,
                                    const void** result) {
  // Copy the data from the protobuf into the Arena.
  uint8_t* data_copy = static_cast<uint8_t*>(arena->AllocateBytes(pb_value.size()));
  memcpy(data_copy, &pb_value[0], pb_value.size());

  // If the type is a STRING, then we need to return a pointer to a Slice
  // element pointing to the string. Otherwise, just verify that the provided
  // value was the right size.
  if (schema.type_info()->type() == STRING) {
    *result = arena->NewObject<Slice>(data_copy, pb_value.size());
  } else {
    // TODO: add test case for this invalid request
    size_t expected_size = schema.type_info()->size();
    if (pb_value.size() != expected_size) {
      return Status::InvalidArgument(
        StringPrintf("Bad predicate on %s. Expected value size %zd, got %zd",
                     schema.ToString().c_str(), expected_size, pb_value.size()));
    }
    *result = data_copy;
  }

  return Status::OK();
}

static Status DecodeEncodedKeyRange(const NewScanRequestPB& scan_pb,
                                    const Schema& tablet_schema,
                                    const SharedScanner& scanner,
                                    ScanSpec* spec) {
  gscoped_ptr<EncodedKey> start, stop;
  if (scan_pb.has_encoded_start_key()) {
    RETURN_NOT_OK_PREPEND(EncodedKey::DecodeEncodedString(
                            tablet_schema, scanner->arena(),
                            scan_pb.encoded_start_key(), &start),
                          "Invalid scan start key");
  }

  if (scan_pb.has_encoded_stop_key()) {
    RETURN_NOT_OK_PREPEND(EncodedKey::DecodeEncodedString(
                            tablet_schema, scanner->arena(),
                            scan_pb.encoded_stop_key(), &stop),
                          "Invalid scan stop key");
  }

  if (scan_pb.order_mode() == ORDERED && scan_pb.has_encoded_last_row_key()) {
    if (start) {
      return Status::InvalidArgument("Cannot specify both a start key and a last key");
    }
    // Set the start key to the last key from a previous scan result.
    RETURN_NOT_OK_PREPEND(EncodedKey::DecodeEncodedString(
                            tablet_schema, scanner->arena(),
                            scan_pb.encoded_last_row_key(), &start),
                          "Failed to decode encoded last row key");
    // Increment the start key, so we don't return the last row again.
    RETURN_NOT_OK_PREPEND(EncodedKey::IncrementEncodedKey(tablet_schema, &start),
                          "Failed to increment encoded last row key");
  }

  if (start) {
    spec->SetLowerBoundKey(start.get());
    scanner->autorelease_pool()->Add(start.release());
  }
  if (stop) {
    spec->SetUpperBoundKey(stop.get());
    scanner->autorelease_pool()->Add(stop.release());
  }

  return Status::OK();
}

static Status SetupScanSpec(const NewScanRequestPB& scan_pb,
                            const Schema& tablet_schema,
                            const Schema& projection,
                            vector<ColumnSchema>* missing_cols,
                            gscoped_ptr<ScanSpec>* spec,
                            const SharedScanner& scanner) {
  gscoped_ptr<ScanSpec> ret(new ScanSpec);

  // First the column range predicates.
  BOOST_FOREACH(const ColumnRangePredicatePB& pred_pb, scan_pb.range_predicates()) {
    if (!pred_pb.has_lower_bound() && !pred_pb.has_upper_bound()) {
      return Status::InvalidArgument(
        string("Invalid predicate ") + pred_pb.ShortDebugString() +
        ": has no lower or upper bound.");
    }
    ColumnSchema col(ColumnSchemaFromPB(pred_pb.column()));
    if (projection.find_column(col.name()) == -1) {
      missing_cols->push_back(col);
    }

    const void* lower_bound = NULL;
    const void* upper_bound = NULL;
    if (pred_pb.has_lower_bound()) {
      const void* val;
      RETURN_NOT_OK(ExtractPredicateValue(col, pred_pb.lower_bound(),
                                          scanner->arena(),
                                          &val));
      lower_bound = val;
    } else {
      lower_bound = NULL;
    }
    if (pred_pb.has_upper_bound()) {
      const void* val;
      RETURN_NOT_OK(ExtractPredicateValue(col, pred_pb.upper_bound(),
                                          scanner->arena(),
                                          &val));
      upper_bound = val;
    } else {
      upper_bound = NULL;
    }

    ColumnRangePredicate pred(col, lower_bound, upper_bound);
    if (VLOG_IS_ON(3)) {
      VLOG(3) << "Parsed predicate " << pred.ToString() << " from " << scan_pb.ShortDebugString();
    }
    ret->AddPredicate(pred);

    ret->set_cache_blocks(scan_pb.cache_blocks());
  }

  // When doing an ordered scan, we need to include the key columns to be able to encode
  // the last row key for the scan response.
  if (scan_pb.order_mode() == kudu::ORDERED &&
      projection.num_key_columns() != tablet_schema.num_key_columns()) {
    for (int i = 0; i < tablet_schema.num_key_columns(); i++) {
      const ColumnSchema &col = tablet_schema.column(i);
      if (projection.find_column(col.name()) == -1) {
        missing_cols->push_back(col);
      }
    }
  }
  // Then any encoded key range predicates.
  RETURN_NOT_OK(DecodeEncodedKeyRange(scan_pb, tablet_schema, scanner, ret.get()));

  spec->swap(ret);
  return Status::OK();
}

// Start a new scan.
Status TabletServiceImpl::HandleNewScanRequest(TabletPeer* tablet_peer,
                                               const ScanRequestPB* req,
                                               const std::string& requestor_string,
                                               ScanResultCollector* result_collector,
                                               std::string* scanner_id,
                                               Timestamp* snap_timestamp,
                                               bool* has_more_results,
                                               TabletServerErrorPB::Code* error_code) {
  DCHECK(result_collector != NULL);
  DCHECK(error_code != NULL);
  DCHECK(req->has_new_scan_request());
  const NewScanRequestPB& scan_pb = req->new_scan_request();
  TRACE_EVENT1("tserver", "TabletServiceImpl::HandleNewScanRequest",
               "tablet_id", scan_pb.tablet_id());

  const shared_ptr<Schema> tablet_schema(tablet_peer->tablet()->schema());

  SharedScanner scanner;
  server_->scanner_manager()->NewScanner(tablet_peer->tablet_id(),
                                         requestor_string,
                                         &scanner);

  // If we early-exit out of this function, automatically unregister
  // the scanner.
  ScopedUnregisterScanner unreg_scanner(server_->scanner_manager(), scanner->id());

  // Create the user's requested projection.
  // TODO: add test cases for bad projections including 0 columns
  Schema projection;
  Status s = ColumnPBsToSchema(scan_pb.projected_columns(), &projection);
  if (PREDICT_FALSE(!s.ok())) {
    *error_code = TabletServerErrorPB::INVALID_SCHEMA;
    return s;
  }

  if (projection.has_column_ids()) {
    *error_code = TabletServerErrorPB::INVALID_SCHEMA;
    return Status::InvalidArgument("User requests should not have Column IDs");
  }

  if (scan_pb.order_mode() == ORDERED) {
    // Ordered scans must be at a snapshot so that we perform a serializable read (which can be
    // resumed). Otherwise, this would be read committed isolation, which is not resumable.
    if (scan_pb.read_mode() != READ_AT_SNAPSHOT) {
      *error_code = TabletServerErrorPB::INVALID_SNAPSHOT;
          return Status::InvalidArgument("Cannot do an ordered scan that is not a snapshot read");
    }
  }

  gscoped_ptr<ScanSpec> spec(new ScanSpec);
  vector<ColumnSchema> missing_cols;
  s = SetupScanSpec(scan_pb, *tablet_schema, projection, &missing_cols, &spec, scanner);
  if (PREDICT_FALSE(!s.ok())) {
    *error_code = TabletServerErrorPB::INVALID_SCAN_SPEC;
    return s;
  }

  // Fix for KUDU-15: if predicate columns are missing from the projection,
  // add to them projection before passing the projection to the iterator and
  // save the original projection (in order to trim the missing predicate columns
  // from the reply to the client).
  if (missing_cols.size() > 0) {
    gscoped_ptr<Schema> orig_projection(new Schema(projection));
    SchemaBuilder projection_builder(projection);
    BOOST_FOREACH(const ColumnSchema& col, missing_cols) {
      projection_builder.AddColumn(col, tablet_schema->is_key_column(col.name()));
    }
    projection = projection_builder.BuildWithoutIds();
    scanner->set_client_projection_schema(orig_projection.Pass());
  }

  gscoped_ptr<RowwiseIterator> iter;
  // Preset the error code for when creating the iterator on the tablet fails
  TabletServerErrorPB::Code tmp_error_code = TabletServerErrorPB::MISMATCHED_SCHEMA;

  {
    TRACE("Creating iterator");
    TRACE_EVENT0("tserver", "Create iterator");

    switch (scan_pb.read_mode()) {
      case READ_LATEST: {
        s = tablet_peer->tablet()->NewRowIterator(projection, &iter);
        break;
      }
      case READ_AT_SNAPSHOT: {
        s = HandleScanAtSnapshot(scan_pb, projection, tablet_peer, &iter, snap_timestamp);
        if (!s.ok()) {
          tmp_error_code = TabletServerErrorPB::INVALID_SNAPSHOT;
        }
      }
        TRACE("Iterator created");
    }
  }

  if (PREDICT_TRUE(s.ok())) {
    TRACE_EVENT0("tserver", "iter->Init");
    s = iter->Init(spec.get());
  }

  TRACE("Iterator init: $0", s.ToString());

  if (PREDICT_FALSE(s.IsInvalidArgument())) {
    // An invalid projection returns InvalidArgument above.
    // TODO: would be nice if we threaded these more specific
    // error codes throughout Kudu.
    *error_code = tmp_error_code;
    return s;
  } else if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << "Error setting up scanner with request " << req->ShortDebugString();
    *error_code = TabletServerErrorPB::UNKNOWN_ERROR;
    return s;
  }

  *has_more_results = iter->HasNext();
  TRACE("has_more: $0", *has_more_results);
  if (!*has_more_results) {
    // If there are no more rows, we can short circuit some work and respond immediately.
    VLOG(1) << "No more rows, short-circuiting out without creating a server-side scanner.";
    return Status::OK();
  }

  scanner->Init(iter.Pass(), spec.Pass());
  unreg_scanner.Cancel();
  *scanner_id = scanner->id();

  VLOG(1) << "Started scanner " << scanner->id() << ": " << scanner->iter()->ToString();

  size_t batch_size_bytes = GetMaxBatchSizeBytesHint(req);
  if (batch_size_bytes > 0) {
    TRACE("Continuing scan request");
    // TODO: instead of copying the pb, instead split HandleContinueScanRequest
    // and call the second half directly
    ScanRequestPB continue_req(*req);
    continue_req.set_scanner_id(scanner->id());
    RETURN_NOT_OK(HandleContinueScanRequest(&continue_req, result_collector, has_more_results,
                                            error_code));
  }
  return Status::OK();
}

// Continue an existing scan request.
Status TabletServiceImpl::HandleContinueScanRequest(const ScanRequestPB* req,
                                                    ScanResultCollector* result_collector,
                                                    bool* has_more_results,
                                                    TabletServerErrorPB::Code* error_code) {
  DCHECK(req->has_scanner_id());
  TRACE_EVENT1("tserver", "TabletServiceImpl::HandleContinueScanRequest",
               "scanner_id", req->scanner_id());

  // TODO: need some kind of concurrency control on these scanner objects
  // in case multiple RPCs hit the same scanner at the same time. Probably
  // just a trylock and fail the RPC if it contends.
  SharedScanner scanner;
  if (!server_->scanner_manager()->LookupScanner(req->scanner_id(), &scanner)) {
    *error_code = TabletServerErrorPB::SCANNER_EXPIRED;
    return Status::NotFound("Scanner not found");
  }
  VLOG(2) << "Found existing scanner " << scanner->id() << " for request: "
          << req->ShortDebugString();
  TRACE("Found scanner $0", scanner->id());

  size_t batch_size_bytes = GetMaxBatchSizeBytesHint(req);

  if (batch_size_bytes == 0 && req->close_scanner()) {
    *has_more_results = false;
    bool success = server_->scanner_manager()->UnregisterScanner(req->scanner_id());
    LOG_IF(WARNING, !success) << "Scanner " << scanner->id() <<
      " not removed successfully from scanner manager. May be a bug.";
    return Status::OK();
  }

  // TODO: check the call_seq_id!

  scanner->UpdateAccessTime();

  RowwiseIterator* iter = scanner->iter();

  // TODO: could size the RowBlock based on the user's requested batch size?
  // If people had really large indirect objects, we would currently overshoot
  // their requested batch size by a lot.
  Arena arena(32 * 1024, 1 * 1024 * 1024);
  RowBlock block(scanner->iter()->schema(),
                 FLAGS_tablet_server_scan_batch_size_rows, &arena);

  // TODO: in the future, use the client timeout to set a budget. For now,
  // just use a half second, which should be plenty to amortize call overhead.
  int budget_ms = 500;
  MonoTime deadline = MonoTime::Now(MonoTime::COARSE);
  deadline.AddDelta(MonoDelta::FromMilliseconds(budget_ms));

  while (iter->HasNext()) {
    Status s = iter->NextBlock(&block);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(WARNING) << "Copying rows from internal iterator for request " << req->ShortDebugString();
      *error_code = TabletServerErrorPB::UNKNOWN_ERROR;
      return s;
    }

    if (PREDICT_TRUE(block.nrows() > 0)) {
      result_collector->HandleRowBlock(scanner->client_projection_schema(), block);
    }

    int64_t response_size = result_collector->BytesRead();

    if (VLOG_IS_ON(2)) {
      // This may be fairly expensive if row block size is small
      TRACE("Copied block (nrows=$0), new size=$1", block.nrows(), response_size);
    }

    // TODO: should check if RPC got cancelled, once we implement RPC cancellation.
    MonoTime now = MonoTime::Now(MonoTime::COARSE);
    if (PREDICT_FALSE(!now.ComesBefore(deadline))) {
      TRACE("Deadline expired - responding early");
      break;
    }

    if (response_size >= batch_size_bytes) {
      break;
    }
  }

  scanner->UpdateAccessTime();
  *has_more_results = !req->close_scanner() && iter->HasNext();
  if (!*has_more_results) {
    VLOG(2) << "Scanner " << scanner->id() << " complete: removing...";
    bool success = server_->scanner_manager()->UnregisterScanner(req->scanner_id());
    LOG_IF(WARNING, !success) << "Scanner " << scanner->id() <<
      " not removed successfully from scanner manager. May be a bug.";
  }

  return Status::OK();
}

Status TabletServiceImpl::HandleScanAtSnapshot(const NewScanRequestPB& scan_pb,
                                               const Schema& projection,
                                               const scoped_refptr<TabletPeer>& tablet_peer,
                                               gscoped_ptr<RowwiseIterator>* iter,
                                               Timestamp* snap_timestamp) {

  // TODO check against the earliest boundary (i.e. how early can we go) right
  // now we're keeping all undos/redos forever!

  // If the client sent a timestamp update our clock with it.
  if (scan_pb.has_propagated_timestamp()) {
    Timestamp propagated_timestamp(scan_pb.propagated_timestamp());

    // Update the clock so that we never generate snapshots lower that
    // 'propagated_timestamp'. If 'propagated_timestamp' is lower than
    // 'now' this call has no effect. If 'propagated_timestamp' is too much
    // into the future this will fail and we abort.
    RETURN_NOT_OK(server_->clock()->Update(propagated_timestamp));
  }

  Timestamp now = server_->clock()->Now();
  Timestamp tmp_snap_timestamp;

  // If the client provided no snapshot timestamp we take the current clock
  // time as the snapshot timestamp.
  if (!scan_pb.has_snap_timestamp()) {
    tmp_snap_timestamp = now;
  // ... else we use the client provided one, but make sure it is less than
  // or equal to the current clock read.
  } else {
    tmp_snap_timestamp.FromUint64(scan_pb.snap_timestamp());
    if (tmp_snap_timestamp.CompareTo(now) > 0) {
      return Status::InvalidArgument("Snapshot time in the future");
    }
  }

  tablet::MvccSnapshot snap;

  // Wait for the in-flights in the snapshot to be finished
  TRACE("Waiting for operations in snapshot to commit");
  MonoTime before = MonoTime::Now(MonoTime::FINE);
  tablet_peer->tablet()->mvcc_manager()->WaitForCleanSnapshotAtTimestamp(tmp_snap_timestamp, &snap);
  uint64_t duration_usec = MonoTime::Now(MonoTime::FINE).GetDeltaSince(before).ToMicroseconds();
  tablet_peer->tablet()->metrics()->snapshot_scan_inflight_wait_duration->Increment(duration_usec);
  TRACE("All operations in snapshot committed. Waited for $0 microseconds", duration_usec);

  tablet::Tablet::OrderMode order;
  switch (scan_pb.order_mode()) {
    case UNORDERED: order = tablet::Tablet::UNORDERED; break;
    case ORDERED: order = tablet::Tablet::ORDERED; break;
    default: LOG(FATAL) << "Unexpected order mode.";
  }
  RETURN_NOT_OK(tablet_peer->tablet()->NewRowIterator(projection, snap,
                                                      order, iter));
  *snap_timestamp = tmp_snap_timestamp;
  return Status::OK();
}

} // namespace tserver
} // namespace kudu
