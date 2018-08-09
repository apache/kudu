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
//

#include "kudu/gutil/macros.h"

// Functions returning default options are declared weak in the runtime
// libraries. To make the linker pick the strong replacements for those
// functions from this module, we explicitly force its inclusion by passing
// -Wl,-u_sanitizer_options_link_helper
extern "C"
void _sanitizer_options_link_helper() { }

// The callbacks we define here will be called from the sanitizer runtime, but
// aren't referenced from the executable. We must ensure that those callbacks
// are not sanitizer-instrumented, and that they aren't stripped by the linker.
#define SANITIZER_HOOK_ATTRIBUTE                                           \
  extern "C"                                                               \
  __attribute__((no_sanitize("address", "memory", "thread", "undefined"))) \
  __attribute__((visibility("default")))                                   \
  __attribute__((used))

#if defined(ADDRESS_SANITIZER)
SANITIZER_HOOK_ATTRIBUTE const char *__asan_default_options() {
  return
#if defined(KUDU_EXTERNAL_SYMBOLIZER_PATH)
  // Overried the symbolizer used when generating reports.
  "external_symbolizer_path=" AS_STRING(KUDU_EXTERNAL_SYMBOLIZER_PATH) " "
#endif
  // Prefixes up to and including this substring will be stripped from source
  // file paths in symbolized reports.
  "strip_path_prefix=/../ ";
}
#endif  // ADDRESS_SANITIZER

#if defined(THREAD_SANITIZER)
SANITIZER_HOOK_ATTRIBUTE const char *__tsan_default_options() {
  return
#if defined(KUDU_EXTERNAL_SYMBOLIZER_PATH)
  // Overried the symbolizer used when generating reports.
  "external_symbolizer_path=" AS_STRING(KUDU_EXTERNAL_SYMBOLIZER_PATH) " "
#endif
  // Flush TSAN memory every 10 seconds
  // this prevents RSS blowup in unit tests which can cause tests to get
  // killed by the OOM killer.
  "flush_memory_ms=10000 "

  // make the history buffer proportional to 2^7 (the maximum value) to
  // keep more stack traces.
  "history_size=7 "

  // Prefixes up to and including this substring will be stripped from source
  // file paths in symbolized reports.
  "strip_path_prefix=/../ ";
}

SANITIZER_HOOK_ATTRIBUTE const char *__tsan_default_suppressions() {
  return
  // libunwind uses some double-checked locking which isn't perfectly safe.
  // Reported at http://savannah.nongnu.org/bugs/index.php?42677
  //
  // With TSAN in clang 3.5, it's the init() function that's flagged as a data
  // race (not local_addr_space_init()), due to the former calling sigfillset()
  // on an unprotected global variable. Although init() calls local_addr_space_init(),
  // it can sometimes be eliminated from the call stack by inlining or a tail-call
  // # optimization, so adding the suppression on both is necessary.
  "race:_ULx86_64_init\n"
  "race:_ULx86_64_local_addr_space_init\n"

  // TODO(todd) After upgrading to clang 6.0, libunwind's cache is getting
  // flagged as unsafe.
  "race:_ULx86_64_step\n"

  // libev uses some lock-free synchronization, but doesn't have TSAN annotations.
  "race:epoll_ctl\n"

  // TSAN complains about data races on the global signals variable in
  // ev_feed_signal and spoiled errno in ev_sighandler. Both are probably noise.
  "race:ev_sighandler\n"

  // See https://github.com/google/glog/issues/80 for a general list of TSAN
  // issues in glog.
  // 1. glog's fatal signal handler isn't signal-safe -- it allocates memory.
  //    This isn't great, but nothing we can do about it. See
  //    https://code.google.com/p/google-glog/issues/detail?id=191
  // 2. LOG(FATAL) from multiple threads can also end up triggering a TSAN error.
  // 3. g_now_entering in stacktrace_libunwind-inl.h is reset to false without
  //    a Release_Store.
  // 4. glog's ANNOTATE_BENIGN_RACE macro doesn't do anything.
  // 5. Mutex::is_safe_ is accessed in an unsafe way.
  // 6. vlocal__ is access in an unsafe way at every VLOG() or VLOG_IS_ON()
  //    call-site.
  "signal:logging_fail\n"
  "race:google::LogMessage::Init\n"
  "race:google::GetStackTrace\n"
  "race:google::InitVLOG3__\n"
  "race:glog_internal_namespace_::Mutex\n"
  "race:vlocal__\n"

  // gflags variables are accessed without synchronization, but FlagSaver and other
  // APIs acquire locks when accessing them. This should be safe on x86 for
  // primitive flag types, but not for string flags, which is why fLS is omitted.
  "race:fLB::\n"
  "race:fLD::\n"
  "race:fLI::\n"
  "race:fLI64::\n"
  "race:fLU64::\n"

  // This method in Boost's UUID library operates on static state with impunity,
  // triggering (harmless) data races in TSAN when boost::uuids::random_generator
  // instances are created across threads (see kudu::ObjectIdGenerator).
  "race:boost::uuids::detail::seed_rng::sha1_random_digest_\n"

  // Squeasel uses ctx->stop_flag to synchronize stopping. It always sets it with
  // the context lock held, but sometimes reads it without the context lock. This
  // should be safe on x86, but is nonetheless flagged by TSAN.
  "race:sq_stop\n"

  // Squeasel reads and frees ctx->listening_sockets without taking any locks. This
  // may be an unsafe race.
  "race:close_all_listening_sockets\n"

  // ------------------------------------------------------------
  // Known bugs below. As these JIRAs are resolved, please remove the relevant
  // suppression.
  // ------------------------------------------------------------

  // KUDU-1283: TSAN warning from consensus OpId
  "race:kudu::consensus::OpId::CopyFrom\n"

  // KUDU-186: sketchy synchronization in catalog manager
  "race:kudu::master::CatalogManager::Shutdown\n"
  "race:kudu::master::CatalogManagerBgTasks::Shutdown\n"
  "race:kudu::master::CatalogManager::~CatalogManager\n"

  // KUDU-574: raft_consensus_quorum-test race on LocalTestPeerProxy destruction
  "race:kudu::consensus::LocalTestPeerProxy::~LocalTestPeerProxy\n"

  // KUDU-569: unsynchronized access to 'state_', 'acceptor_pools_', in
  // GetBoundAddresses()
  "race:kudu::Webserver::GetBoundAddresses\n"
  "race:kudu::RpcServer::GetBoundAddresses\n"

  // KUDU-2439: OpenSSL 1.1's atexit() handler may destroy global state while a
  // Messenger is shutting down and still accessing that state. See
  // https://github.com/openssl/openssl/issues/6214 for more details.
  //
  // This is carried out by OPENSSL_cleanup, but TSAN's unwinder doesn't
  // include any stack frame above the libcrypto lock destruction or memory release
  // call for some reason, so we have to do something more generic.
  "called_from_lib:libcrypto.so\n";
}
#endif  // THREAD_SANITIZER

#if defined(LEAK_SANITIZER)
SANITIZER_HOOK_ATTRIBUTE const char *__lsan_default_options() {
  return
#if defined(KUDU_EXTERNAL_SYMBOLIZER_PATH)
  // Overried the symbolizer used when generating reports.
  "external_symbolizer_path=" AS_STRING(KUDU_EXTERNAL_SYMBOLIZER_PATH) " "
#endif
  // Prefixes up to and including this substring will be stripped from source
  // file paths in symbolized reports.
  "strip_path_prefix=/../ ";
}

SANITIZER_HOOK_ATTRIBUTE const char *__lsan_default_suppressions() {
  return
  // False positive from atexit() registration in libc
  "leak:*__new_exitfn*\n"

  // False positive from krb5 < 1.12
  // Fixed by upstream commit 379d39c17b8930718e98185a5b32a0f7f3e3b4b6
  "leak:krb5_authdata_import_attributes\n";
}
#endif  // LEAK_SANITIZER

#if defined(UNDEFINED_SANITIZER)
SANITIZER_HOOK_ATTRIBUTE const char* __ubsan_default_options() {
  return
#if defined(KUDU_EXTERNAL_SYMBOLIZER_PATH)
  // Overried the symbolizer used when generating reports.
  "external_symbolizer_path=" AS_STRING(KUDU_EXTERNAL_SYMBOLIZER_PATH) " "
#endif
  // Print the stacktrace when UBSan reports an error.
  "print_stacktrace=1 "
  // Prefixes up to and including this substring will be stripped from source
  // file paths in symbolized reports.
  "strip_path_prefix=/../ ";
}
#endif  // UNDEFINED_SANITIZER
