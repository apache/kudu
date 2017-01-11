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

#include "kudu/security/openssl_util.h"

#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <openssl/err.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>

#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/mutex.h"
#include "kudu/util/thread.h"

using std::ostringstream;
using std::string;
using std::vector;

namespace kudu {
namespace security {

namespace {

vector<Mutex*> kCryptoLocks;

// Lock/Unlock the nth lock. Only to be used by OpenSSL.
void LockingCB(int mode, int type, const char* /*file*/, int /*line*/) {
  DCHECK(!kCryptoLocks.empty());
  Mutex* m = kCryptoLocks[type];
  DCHECK(m);
  if (mode & CRYPTO_LOCK) {
    m->lock();
  } else {
    m->unlock();
  }
}

// Return the current pthread's tid. Only to be used by OpenSSL.
void ThreadIdCB(CRYPTO_THREADID* tid) {
  CRYPTO_THREADID_set_numeric(tid, Thread::UniqueThreadId());
}

void DoInitializeOpenSSL() {
  SSL_load_error_strings();
  SSL_library_init();
  OpenSSL_add_all_algorithms();
  RAND_poll();

  // Initialize the OpenSSL mutexes. We intentionally leak these, so ignore
  // LSAN warnings.
  debug::ScopedLeakCheckDisabler d;
  int num_locks = CRYPTO_num_locks();
  kCryptoLocks.reserve(num_locks);
  for (int i = 0; i < num_locks; i++) {
    kCryptoLocks.emplace_back(new Mutex());
  }

  // Callbacks used by OpenSSL required in a multi-threaded setting.
  CRYPTO_set_locking_callback(LockingCB);
  CRYPTO_THREADID_set_callback(ThreadIdCB);
}
} // namespace

void InitializeOpenSSL() {
  static std::once_flag ssl_once;
  std::call_once(ssl_once, DoInitializeOpenSSL);
}

string GetOpenSSLErrors() {
  ostringstream serr;
  uint32_t l;
  int line, flags;
  const char *file, *data;
  bool is_first = true;
  while ((l = ERR_get_error_line_data(&file, &line, &data, &flags)) != 0) {
    if (is_first) {
      is_first = false;
    } else {
      serr << " ";
    }

    char buf[256];
    ERR_error_string_n(l, buf, sizeof(buf));
    serr << buf << ":" << file << ":" << line;
    if (flags & ERR_TXT_STRING) {
      serr << ":" << data;
    }
  }
  return serr.str();
}

} // namespace security
} // namespace kudu
