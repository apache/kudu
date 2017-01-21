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

#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <openssl/err.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/errno.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

using std::ostringstream;
using std::string;
using std::vector;
using strings::Substitute;

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

} // anonymous namespace

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

string GetSSLErrorDescription(int error_code) {
  switch (error_code) {
    case SSL_ERROR_NONE: return "";
    case SSL_ERROR_ZERO_RETURN: return "SSL_ERROR_ZERO_RETURN";
    case SSL_ERROR_WANT_READ: return "SSL_ERROR_WANT_READ";
    case SSL_ERROR_WANT_WRITE: return "SSL_ERROR_WANT_WRITE";
    case SSL_ERROR_WANT_CONNECT: return "SSL_ERROR_WANT_CONNECT";
    case SSL_ERROR_WANT_ACCEPT: return "SSL_ERROR_WANT_ACCEPT";
    case SSL_ERROR_WANT_X509_LOOKUP: return "SSL_ERROR_WANT_X509_LOOKUP";
    case SSL_ERROR_SYSCALL: {
      string queued_error = GetOpenSSLErrors();
      if (!queued_error.empty()) {
        return queued_error;
      }
      return kudu::ErrnoToString(errno);
    };
    default: return GetOpenSSLErrors();
  }
}

namespace {

// Writing the private key from an EVP_PKEY has a different
// signature than the rest of the write functions, so we
// have to provide this wrapper.
int PemWritePrivateKey(BIO* bio, EVP_PKEY* key) {
  auto rsa = ssl_make_unique(EVP_PKEY_get1_RSA(key));
  return PEM_write_bio_RSAPrivateKey(
      bio, rsa.get(), nullptr, nullptr, 0, nullptr, nullptr);
}

} // anonymous namespace

template<> struct SslTypeTraits<BIO> {
  static constexpr auto free = &BIO_free;
};
template<> struct SslTypeTraits<BIGNUM> {
  static constexpr auto free = &BN_free;
};
template<> struct SslTypeTraits<EVP_PKEY> {
  static constexpr auto free = &EVP_PKEY_free;
  static constexpr auto read_pem = &PEM_read_bio_PrivateKey;
  static constexpr auto read_der = &d2i_PrivateKey_bio;
  static constexpr auto write_pem = &PemWritePrivateKey;
  static constexpr auto write_der = &i2d_PrivateKey_bio;
};
template<> struct SslTypeTraits<RSA> {
  static constexpr auto free = &RSA_free;
};
template<> struct SslTypeTraits<X509> {
  static constexpr auto free = &X509_free;
  static constexpr auto read_pem = &PEM_read_bio_X509;
  static constexpr auto read_der = &d2i_X509_bio;
  static constexpr auto write_pem = &PEM_write_bio_X509;
  static constexpr auto write_der = &i2d_X509_bio;
};
template<> struct SslTypeTraits<X509_REQ> {
  static constexpr auto free = &X509_REQ_free;
  static constexpr auto read_pem = &PEM_read_bio_X509_REQ;
  static constexpr auto read_der = &d2i_X509_REQ_bio;
  static constexpr auto write_pem = &PEM_write_bio_X509_REQ;
  static constexpr auto write_der = &i2d_X509_REQ_bio;
};

namespace {

template<class TYPE>
Status ToBIO(BIO* bio, DataFormat format, TYPE* obj) {
  using Traits = SslTypeTraits<TYPE>;
  CHECK(bio);
  CHECK(obj);
  switch (format) {
    case DataFormat::DER:
      CERT_RET_NOT_OK(Traits::write_der(bio, obj),
          "error exporting data in DER format");
      break;
    case DataFormat::PEM:
      CERT_RET_NOT_OK(Traits::write_pem(bio, obj),
          "error exporting data in PEM format");
      break;
  }
  CERT_RET_NOT_OK(BIO_flush(bio), "error flushing BIO");
  return Status::OK();
}

template<class TYPE>
Status FromBIO(BIO* bio, DataFormat format, c_unique_ptr<TYPE>* ret) {
  using Traits = SslTypeTraits<TYPE>;
  CHECK(bio);
  switch (format) {
    case DataFormat::DER:
      *ret = ssl_make_unique(Traits::read_der(bio, nullptr));
      break;
    case DataFormat::PEM:
      *ret = ssl_make_unique(Traits::read_pem(bio, nullptr, nullptr, nullptr));
      break;
  }
  if (PREDICT_FALSE(!*ret)) {
    return Status::RuntimeError(GetOpenSSLErrors());
  }
  return Status::OK();
}

} // anonymous namespace


const string& DataFormatToString(DataFormat fmt) {
  static const string kStrFormatUnknown = "UNKNOWN";
  static const string kStrFormatDer = "DER";
  static const string kStrFormatPem = "PEM";
  switch (fmt) {
    case DataFormat::DER:
      return kStrFormatDer;
    case DataFormat::PEM:
      return kStrFormatPem;
    default:
      return kStrFormatUnknown;
  }
}

Status BasicWrapper::FromFile(const string& fpath, DataFormat format) {
  auto bio = ssl_make_unique(BIO_new(BIO_s_file()));
  CERT_RET_NOT_OK(BIO_read_filename(bio.get(), fpath.c_str()),
                  Substitute("$0: could not read from file", fpath));
  RETURN_NOT_OK_PREPEND(FromBIO(bio.get(), format),
                        Substitute("$0: unable to load data key from file",
                                   fpath));
  return Status::OK();
}

Status BasicWrapper::FromString(const string& data, DataFormat format) {
  const void* mdata = reinterpret_cast<const void*>(data.data());
  auto bio = ssl_make_unique(BIO_new_mem_buf(
#if OPENSSL_VERSION_NUMBER < 0x10002000L
      const_cast<void*>(mdata),
#else
      mdata,
#endif
      data.size()));
  RETURN_NOT_OK_PREPEND(FromBIO(bio.get(), format),
                        "unable to load data from memory");
  return Status::OK();
}

Status BasicWrapper::ToString(std::string* data, DataFormat format) const {
  CHECK(data);
  auto bio = ssl_make_unique(BIO_new(BIO_s_mem()));
  RETURN_NOT_OK_PREPEND(ToBIO(bio.get(), format), "error serializing data");
  BUF_MEM* membuf;
  CERT_CHECK_OK(BIO_get_mem_ptr(bio.get(), &membuf));
  data->assign(membuf->data, membuf->length);
  return Status::OK();
}

void Key::AdoptRawData(RawDataType* data) {
  data_ = ssl_make_unique(data);
}

Status Key::FromBIO(BIO* bio, DataFormat format) {
  RETURN_NOT_OK_PREPEND(::kudu::security::FromBIO(bio, format, &data_),
      "unable to read private key");
  return Status::OK();
}

Status Key::ToBIO(BIO* bio, DataFormat format) const {
  RETURN_NOT_OK_PREPEND(::kudu::security::ToBIO(bio, format, data_.get()),
      "could not export cert");
  return Status::OK();
}

void Cert::AdoptRawData(RawDataType* data) {
  data_ = ssl_make_unique(data);
}

Status Cert::FromBIO(BIO* bio, DataFormat format) {
  RETURN_NOT_OK_PREPEND(::kudu::security::FromBIO(bio, format, &data_),
      "could not read cert");
  return Status::OK();
}

Status Cert::ToBIO(BIO* bio, DataFormat format) const {
  RETURN_NOT_OK_PREPEND(::kudu::security::ToBIO(bio, format, data_.get()),
      "could not export cert");
  return Status::OK();
}

void CertSignRequest::AdoptRawData(RawDataType* data) {
  data_ = ssl_make_unique(data);
}

Status CertSignRequest::FromBIO(BIO* bio, DataFormat format) {
  RETURN_NOT_OK_PREPEND(::kudu::security::FromBIO(bio, format, &data_),
      "could not read X509 CSR");
  return Status::OK();
}

Status CertSignRequest::ToBIO(BIO* bio, DataFormat format) const {
  RETURN_NOT_OK_PREPEND(::kudu::security::ToBIO(bio, format, data_.get()),
      "could not export X509 CSR");
  return Status::OK();
}

Status GeneratePrivateKey(int num_bits, Key* ret) {
  CHECK(ret);
  InitializeOpenSSL();
  auto key = ssl_make_unique(EVP_PKEY_new());
  {
    auto bn = ssl_make_unique(BN_new());
    CERT_CHECK_OK(BN_set_word(bn.get(), RSA_F4));
    auto rsa = ssl_make_unique(RSA_new());
    CERT_RET_NOT_OK(RSA_generate_key_ex(rsa.get(), num_bits, bn.get(), nullptr),
                    "error generating RSA key");
    CERT_RET_NOT_OK(EVP_PKEY_set1_RSA(key.get(), rsa.get()),
                    "error assigning RSA key");
  }
  ret->AdoptRawData(key.release());

  return Status::OK();
}

} // namespace security
} // namespace kudu
