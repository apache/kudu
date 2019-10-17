// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/server/generic_service.h"

#include <ostream>
#include <string>
#include <unordered_set>
#include <utility>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/clock/mock_ntp.h"
#include "kudu/clock/time_service.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h" // IWYU pragma: keep
#include "kudu/rpc/rpc_context.h"
#include "kudu/server/server_base.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/debug/leak_annotations.h" // IWYU pragma: keep
#include "kudu/util/flag_tags.h"
#include "kudu/util/flags.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/status.h"

DECLARE_string(time_source);
DECLARE_bool(use_hybrid_clock);

using std::string;
using std::unordered_set;

namespace kudu {
namespace server {

GenericServiceImpl::GenericServiceImpl(ServerBase* server)
  : GenericServiceIf(server->metric_entity(), server->result_tracker()),
    server_(server) {
}

GenericServiceImpl::~GenericServiceImpl() {
}

bool GenericServiceImpl::AuthorizeSuperUser(const google::protobuf::Message* /*req*/,
                                            google::protobuf::Message* /*resp*/,
                                            rpc::RpcContext* rpc) {
  return server_->Authorize(rpc, ServerBase::SUPER_USER);
}

bool GenericServiceImpl::AuthorizeClient(const google::protobuf::Message* /*req*/,
                                         google::protobuf::Message* /*resp*/,
                                         rpc::RpcContext* rpc) {
  return server_->Authorize(rpc, ServerBase::SUPER_USER | ServerBase::USER);
}


void GenericServiceImpl::GetFlags(const GetFlagsRequestPB* req,
                                  GetFlagsResponsePB* resp,
                                  rpc::RpcContext* rpc) {
  // If no tags were specified, return all flags that have non-default values.
  // If flags were specified, will ignore 'all_flags'.
  // If tags were specified, also filter flags that don't match any tag.
  bool all_flags = req->all_flags();
  unordered_set<string> flags(req->flags().begin(), req->flags().end());
  for (const auto& entry : GetFlagsMap()) {
    if (entry.second.is_default && !all_flags && flags.empty()) {
      continue;
    }

    if (!flags.empty() && !ContainsKey(flags, entry.first)) {
      continue;
    }

    unordered_set<string> tags;
    GetFlagTags(entry.first, &tags);
    bool matches = req->tags().empty();
    for (const auto& tag : req->tags()) {
      if (ContainsKey(tags, tag)) {
        matches = true;
        break;
      }
    }
    if (!matches) continue;

    auto* flag = resp->add_flags();
    flag->set_name(entry.first);
    flag->set_value(CheckFlagAndRedact(entry.second, EscapeMode::NONE));
    flag->set_is_default_value(entry.second.current_value == entry.second.default_value);
    for (const auto& tag : tags) {
      flag->add_tags(tag);
    }
  }
  rpc->RespondSuccess();
}

void GenericServiceImpl::SetFlag(const SetFlagRequestPB* req,
                                 SetFlagResponsePB* resp,
                                 rpc::RpcContext* rpc) {

  // Validate that the flag exists and get the current value.
  string old_val;
  if (!google::GetCommandLineOption(req->flag().c_str(),
                                    &old_val)) {
    resp->set_result(SetFlagResponsePB::NO_SUCH_FLAG);
    rpc->RespondSuccess();
    return;
  }

  // Validate that the flag is runtime-changeable.
  unordered_set<string> tags;
  GetFlagTags(req->flag(), &tags);
  if (!ContainsKey(tags, "runtime")) {
    if (req->force()) {
      LOG(WARNING) << rpc->requestor_string() << " forcing change of "
                   << "non-runtime-safe flag " << req->flag();
    } else {
      resp->set_result(SetFlagResponsePB::NOT_SAFE);
      resp->set_msg("Flag is not safe to change at runtime");
      rpc->RespondSuccess();
      return;
    }
  }

  resp->set_old_value(old_val);

  // Try to set the new value.
  string ret = google::SetCommandLineOption(
      req->flag().c_str(),
      req->value().c_str());
  if (ret.empty()) {
    resp->set_result(SetFlagResponsePB::BAD_VALUE);
    resp->set_msg("Unable to set flag: bad value");
  } else {
    LOG(INFO) << rpc->requestor_string() << " changed flags via RPC: "
              << req->flag() << " from '" << old_val << "' to '"
              << req->value() << "'";
    resp->set_result(SetFlagResponsePB::SUCCESS);
    resp->set_msg(ret);
  }

  rpc->RespondSuccess();
}

void GenericServiceImpl::CheckLeaks(const CheckLeaksRequestPB* /*req*/,
                                    CheckLeaksResponsePB* resp,
                                    rpc::RpcContext* rpc) {
  // We have to use these nested #if statements rather than an && to avoid
  // a preprocessor error with GCC which doesn't know about __has_feature.
#if defined(__has_feature)
#  if __has_feature(address_sanitizer)
#    define LSAN_ENABLED
#  endif
#endif
#ifndef LSAN_ENABLED
  resp->set_success(false);
#else
  LOG(INFO) << "Checking for leaks (request via RPC)";
  resp->set_success(true);

  // Workaround for LSAN issue 757 (leak check can give false positives when
  // run concurrently with other work). If LSAN reports a leak, we'll retry
  // a few times and see if it goes away on its own. Any real leak would,
  // by definition, not resolve itself over time.
  //
  // See https://github.com/google/sanitizers/issues/757
  bool has_leaks = true;
  for (int i = 0; i < 5; i++) {
    has_leaks = __lsan_do_recoverable_leak_check();
    if (!has_leaks) {
      if (i > 0) {
        LOG(WARNING) << strings::Substitute("LeakSanitizer found a leak that went away after $0 "
                                            "retries.\n", i)
                     << "Treating it as a false positive and ignoring it.";
      }
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(5));
  }

  resp->set_found_leaks(has_leaks);
#endif
#undef LSAN_ENABLED
  rpc->RespondSuccess();
}

void GenericServiceImpl::FlushCoverage(const FlushCoverageRequestPB* /*req*/,
                                       FlushCoverageResponsePB* resp,
                                       rpc::RpcContext* rpc) {
  if (IsCoverageBuild()) {
    TryFlushCoverage();
    LOG(INFO) << "Flushed coverage info. (request from " << rpc->requestor_string() << ")";
    resp->set_success(true);
  } else {
    LOG(WARNING) << "Non-coverage build cannot flush coverage (request from "
                 << rpc->requestor_string() << ")";
    resp->set_success(false);
  }
  rpc->RespondSuccess();
}

void GenericServiceImpl::ServerClock(const ServerClockRequestPB* req,
                                     ServerClockResponsePB* resp,
                                     rpc::RpcContext* rpc) {
  resp->set_timestamp(server_->clock()->Now().ToUint64());
  rpc->RespondSuccess();
}

void GenericServiceImpl::SetServerWallClockForTests(const SetServerWallClockForTestsRequestPB *req,
                                                   SetServerWallClockForTestsResponsePB *resp,
                                                   rpc::RpcContext *context) {
  if (!FLAGS_use_hybrid_clock || FLAGS_time_source != "mock") {
    LOG(WARNING) << "Error setting wall clock for tests. Server is not using HybridClock"
        "or was not started with '--ntp-source=mock'";
    resp->set_success(false);
  }

  auto* clock = down_cast<clock::HybridClock*>(server_->clock());
  auto* mock = down_cast<clock::MockNtp*>(clock->time_service());
  if (req->has_now_usec()) {
    mock->SetMockClockWallTimeForTests(req->now_usec());
  }
  if (req->has_max_error_usec()) {
    mock->SetMockMaxClockErrorForTests(req->max_error_usec());
  }
  resp->set_success(true);
  context->RespondSuccess();
}

void GenericServiceImpl::GetStatus(const GetStatusRequestPB* /*req*/,
                                   GetStatusResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  // Note: we must ensure that resp->has_status() is true in all cases to
  // preserve backwards compatibility because it is defined as a required field.
  Status s = server_->GetStatusPB(resp->mutable_status());
  if (!s.ok()) {
    StatusToPB(s, resp->mutable_error());
  }
  rpc->RespondSuccess();
}

void GenericServiceImpl::DumpMemTrackers(const DumpMemTrackersRequestPB* /*req*/,
                                         DumpMemTrackersResponsePB* resp,
                                         rpc::RpcContext* rpc) {
  MemTracker::TrackersToPb(resp->mutable_root_tracker());
  rpc->RespondSuccess();
}

} // namespace server
} // namespace kudu
