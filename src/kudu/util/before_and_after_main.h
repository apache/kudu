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

// This file is intended for direct inclusion into .cc files where the main()
// function is defined, so no 'pragma once' or include guard is needed.
//
// This approach addresses KUDU-3635 for OpenSSL versions 1.1.1 and newer
// and serves as a stop-gap for KUDU-2439.

#include <functional>

#include <glog/logging.h>
#include <glog/raw_logging.h>
#if defined(TCMALLOC_ENABLED)
#include <gperftools/tcmalloc_guard.h>
#endif

#if !defined(KUDU_TEST_MAIN)
#include "kudu/rpc/messenger.h"
#endif
#include "kudu/util/entry_exit_wrapper.h"
#if !defined(KUDU_TEST_MAIN)
#include "kudu/util/monotime.h"
#endif
#include "kudu/util/openssl_util.h"
#include "kudu/util/process_memory.h"

static void module_init_tcmalloc() {
#if defined(TCMALLOC_ENABLED) && !defined(NDEBUG)
  // Just a sanity check for non-release builds: make sure tcmalloc has already
  // installed its shims and isn't barfing after TCMallocGuard's constructor.
  RAW_DCHECK(kudu::process_memory::GetTCMallocCurrentAllocatedBytes() != 0,
             "tcmalloc should be fully functional at this point");
#endif
}

static void module_fini_tcmalloc() {
#if defined(TCMALLOC_ENABLED) && !defined(NDEBUG)
  // Non-release builds only: as an extra sanity check, poke tcmalloc a bit
  // to make sure it's still operational and not barfing at this point.
  RAW_VLOG(2, "invoking tcmalloc GC");
  kudu::process_memory::GcTcmalloc();
#endif
}

static void module_init_openssl() {
  // Make sure OpenSSL hasn't been yet initialized by Kudu at this point.
  RAW_DCHECK(!kudu::security::IsOpenSSLInitialized(),
             "OpenSSL shouldn't be initialized yet");

  // Mark the OpenSSL to be initialized with the OPENSSL_INIT_NO_ATEXIT option,
  // i.e. instruct the OpenSSL not to register its atexit() hook because
  // the application will call OPENSSL_cleanup() itself.
  kudu::security::SetStandaloneInit(true);
#if !defined(KUDU_TEST_MAIN)
  // For kudu-{master,tserver} and kudu CLI binary, explicitly initialize
  // the OpenSSL library before the main() function. As for the Kudu's tests,
  // they implicitly initialize the library on-demand from the context of their
  // main() function in test_main.cc, if needed.
  kudu::security::InitializeOpenSSL();
#endif
}

static void module_fini_openssl() {
#if !defined(KUDU_TEST_MAIN)
  // This is a stop-gap to work around KUDU-2439. For the kudu CLI and
  // kudu-{master,tserver} binaries, wait a bit for all Messenger instances
  // to be shut down. Otherwise, if the asynchronous Messenger's shutdown
  // process is still in progress, it might issue calls to the OpenSSL's runtime
  // when destructing the Messenger::tls_context_ field. Meanwhile, the OpenSSL
  // library could be shut down already or in the process of doing so,
  // and a data race like that would often end up in undefined behavior or
  // a crash with SIGSEGV/SIGFPE/SIGABRT. Introducing a bit of latency into
  // the kudu-{master,tserver} shutdown isn't a bit deal. Also, in the vast
  // majority of the kudu CLI's use cases, it's better to incur an extra second
  // of latency compared with a crash and inability to tell whether the tool
  // succeeded or not by analyzing its exit code.
  if (const auto mc = kudu::rpc::Messenger::GetInstanceCount(); mc != 0) {
    RAW_VLOG(2, "waiting for %d Messengers to shut down", mc);
    const auto deadline = kudu::MonoTime::Now() + kudu::MonoDelta::FromSeconds(1);
    while (kudu::MonoTime::Now() < deadline) {
      SleepFor(kudu::MonoDelta::FromMilliseconds(50));
      if (kudu::rpc::Messenger::GetInstanceCount() == 0) {
        break;
      }
    }
  }
#endif // #if !defined(KUDU_TEST_MAIN) ...

  // Call OPENSSL_cleanup() to release resources and clean up the global state
  // of the library: it's applicable to OpenSSL 1.1.1 and newer versions.
  // At this point, tcmalloc must still be operational.
  RAW_VLOG(2, "cleaning up OpenSSL runtime");
  kudu::security::FinalizeOpenSSL();
}

#if defined(TCMALLOC_ENABLED)
// Make sure tcmalloc is up and running, and it has already installed shims
// for all the necessary symbols. Maybe, that's too much and just having symbols
// set by the linker when linking and loading libtcmalloc should be good enough,
// but this approach with TCMallocGuard is guaranteed bulletproof.
static TCMallocGuard g_tcmalloc_initializer;
#endif
// Make sure tcmalloc is opened and initialized prior to initializing OpenSSL's
// runtime before entering main().
static kudu::util::EntryExitWrapper g_wrapper_tcmalloc(module_init_tcmalloc,
                                                       module_fini_tcmalloc);
// After exiting from main(), clean up the OpenSSL's library global state.
static kudu::util::EntryExitWrapper g_wrapper_openssl(module_init_openssl,
                                                      module_fini_openssl);
