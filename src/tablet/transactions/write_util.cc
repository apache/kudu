// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/write_util.h"

#include "common/wire_protocol.h"
#include "common/schema.h"
#include "consensus/consensus.pb.h"

namespace kudu {
namespace tablet {

using consensus::CommitMsg;
using consensus::OP_ABORT;
using consensus::WRITE_OP;
using tserver::TabletServerErrorPB;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;

void SetupClientError(TabletServerErrorPB* error,
                       const Status& s,
                       TabletServerErrorPB::Code code) {
  StatusToPB(s, error->mutable_status());
  error->set_code(code);
}

Status DecodeRowBlockAndSetupClientErrors(RowwiseRowBlockPB* block_pb,
                      WriteResponsePB* resp,
                      rpc::RpcContext* context,
                      const Schema& tablet_key_projection,
                      bool is_inserts_block,
                      Schema* client_schema,
                      vector<const uint8_t*>* row_block) {
  // Extract the schema of the row block
  Status s = ColumnPBsToSchema(block_pb->schema(), client_schema);
  if (!s.ok()) {
    SetupClientError(resp->mutable_error(), s,
               TabletServerErrorPB::INVALID_SCHEMA);
    return s;
  }

  // Check that the schema sent by the user matches the key projection of the tablet.
  Schema client_key_projection = client_schema->CreateKeyProjection();
  if (!client_key_projection.Equals(tablet_key_projection)) {
    s = Status::InvalidArgument("Mismatched key projection schema, expected",
                                tablet_key_projection.ToString());
    SetupClientError(resp->mutable_error(), s,
               TabletServerErrorPB::MISMATCHED_SCHEMA);
    return s;
  }

  // Extract the row block
  if (is_inserts_block) {
    s = ExtractRowsFromRowBlockPB(*client_schema, block_pb, row_block);
  } else {
    s = ExtractRowsFromRowBlockPB(client_key_projection, block_pb, row_block);
  }

  if (!s.ok()) {
    SetupClientError(resp->mutable_error(), s,
               TabletServerErrorPB::INVALID_ROW_BLOCK);
    return s;
  }
  return Status::OK();
}

MutationResultPB::MutationTypePB MutationType(const MutationResultPB* result) {
  if (result->mutations_size() == 0) {
    return MutationResultPB::NO_MUTATION;
  }
  if (result->mutations_size() == 1) {
    return result->mutations(0).has_mrs_id() ?
        MutationResultPB::MRS_MUTATION :
        MutationResultPB::DELTA_MUTATION;
  }
  DCHECK_EQ(result->mutations_size(), 2);
  return MutationResultPB::DUPLICATED_MUTATION;
}

}  // namespace tablet
}  // namespace kudu
