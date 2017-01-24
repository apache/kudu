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

#pragma once

#include "kudu/security/openssl_util.h"

#include <string>

#include <glog/logging.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {

template<> struct SslTypeTraits<BIO> {
  static constexpr auto free = &BIO_free;
};

template<typename TYPE, typename Traits = SslTypeTraits<TYPE>>
Status ToBIO(BIO* bio, DataFormat format, TYPE* obj) {
  CHECK(bio);
  CHECK(obj);
  switch (format) {
    case DataFormat::DER:
      OPENSSL_RET_NOT_OK(Traits::write_der(bio, obj),
          "error exporting data in DER format");
      break;
    case DataFormat::PEM:
      OPENSSL_RET_NOT_OK(Traits::write_pem(bio, obj),
          "error exporting data in PEM format");
      break;
  }
  OPENSSL_RET_NOT_OK(BIO_flush(bio), "error flushing BIO");
  return Status::OK();
}

template<typename TYPE, typename Traits = SslTypeTraits<TYPE>>
Status FromBIO(BIO* bio, DataFormat format, c_unique_ptr<TYPE>* ret) {
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

template<typename Type, typename Traits = SslTypeTraits<Type>>
Status FromString(const string& data, DataFormat format,
                  c_unique_ptr<Type>* ret) {
  const void* mdata = reinterpret_cast<const void*>(data.data());
  auto bio = ssl_make_unique(BIO_new_mem_buf(
#if OPENSSL_VERSION_NUMBER < 0x10002000L
      const_cast<void*>(mdata),
#else
      mdata,
#endif
      data.size()));
  RETURN_NOT_OK_PREPEND((FromBIO<Type, Traits>(bio.get(), format, ret)),
                        "unable to load data from memory");
  return Status::OK();
}

template<typename Type, typename Traits = SslTypeTraits<Type>>
Status ToString(std::string* data, DataFormat format, Type* obj) {
  CHECK(data);
  auto bio = ssl_make_unique(BIO_new(BIO_s_mem()));
  RETURN_NOT_OK_PREPEND((ToBIO<Type, Traits>(bio.get(), format, obj)),
                        "error serializing data");
  BUF_MEM* membuf;
  OPENSSL_CHECK_OK(BIO_get_mem_ptr(bio.get(), &membuf));
  data->assign(membuf->data, membuf->length);
  return Status::OK();
}

template<typename Type, typename Traits = SslTypeTraits<Type>>
Status FromFile(const string& fpath, DataFormat format,
                c_unique_ptr<Type>* ret) {
  auto bio = ssl_make_unique(BIO_new(BIO_s_file()));
  OPENSSL_RET_NOT_OK(BIO_read_filename(bio.get(), fpath.c_str()),
      strings::Substitute("could not read data from file '$0'", fpath));
  RETURN_NOT_OK_PREPEND((FromBIO<Type, Traits>(bio.get(), format, ret)),
      strings::Substitute("unable to load data from file '$0'", fpath));
  return Status::OK();
}

} // namespace security
} // namespace kudu
