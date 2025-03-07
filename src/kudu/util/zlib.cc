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

#include "kudu/util/zlib.h"

#include <cstdint>
#include <cstring>
#include <string>

#include <glog/logging.h>
#include <zconf.h>
#include <zlib.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using std::ios;
using std::ostream;
using std::string;

#define ZRETURN_NOT_OK(call) \
  RETURN_NOT_OK(ZlibResultToStatus(call))

namespace kudu {
namespace zlib {

namespace  {
Status ZlibResultToStatus(int rc) {
  switch (rc) {
    case Z_OK:
      return Status::OK();
    case Z_STREAM_END:
      return Status::EndOfFile("zlib EOF");
    case Z_NEED_DICT:
      return Status::Corruption("zlib error: NEED_DICT");
    case Z_ERRNO:
      return Status::IOError("zlib error: Z_ERRNO");
    case Z_STREAM_ERROR:
      return Status::Corruption("zlib error: STREAM_ERROR");
    case Z_DATA_ERROR:
      return Status::Corruption("zlib error: DATA_ERROR");
    case Z_MEM_ERROR:
      return Status::RuntimeError("zlib error: MEM_ERROR");
    case Z_BUF_ERROR:
      return Status::RuntimeError("zlib error: BUF_ERROR");
    case Z_VERSION_ERROR:
      return Status::RuntimeError("zlib error: VERSION_ERROR");
    default:
      return Status::RuntimeError(
          strings::Substitute("zlib error: unknown error $0", rc));
  }
}
} // anonymous namespace

Status Compress(Slice input, ostream* out) {
  return CompressLevel(input, Z_BEST_SPEED, out);
}

// See https://zlib.net/zlib_how.html for context on using zlib.
Status CompressLevel(Slice input, int level, ostream* out) {
  DCHECK(out);
  // Output stream exceptions aren't handled in this function, so the stream
  // isn't supposed to throw on errors, but just set bad/failure bits instead.
  DCHECK_EQ(ios::goodbit, out->exceptions());
  DCHECK(level >= Z_DEFAULT_COMPRESSION && level <= Z_BEST_COMPRESSION);

  z_stream zs;
  memset(&zs, 0, sizeof(zs));
  ZRETURN_NOT_OK(deflateInit2(&zs, level, Z_DEFLATED,
                              MAX_WBITS + 16 /* enable gzip */,
                              8 /* memory level, max is 9 */,
                              Z_DEFAULT_STRATEGY));

  zs.avail_in = input.size();
  zs.next_in = const_cast<uint8_t*>(input.data());
  constexpr const size_t kChunkSize = 16 * 1024;
  unsigned char buf[kChunkSize];
  int rc;
  do {
    zs.avail_out = kChunkSize;
    zs.next_out = buf;
    const int flush = (zs.avail_in == 0) ? Z_FINISH : Z_NO_FLUSH;
    rc = deflate(&zs, flush);
    if (PREDICT_FALSE(rc != Z_OK && rc != Z_STREAM_END)) {
      ignore_result(deflateEnd(&zs));
      return ZlibResultToStatus(rc);
    }
    DCHECK_GT(zs.next_out - buf, 0);
    out->write(reinterpret_cast<char*>(buf), zs.next_out - buf);
    if (PREDICT_FALSE(out->fail())) {
      ignore_result(deflateEnd(&zs));
      return Status::IOError("error writing to output stream");
    }
  } while (rc != Z_STREAM_END);

  return ZlibResultToStatus(deflateEnd(&zs));
}

// See https://zlib.net/zlib_how.html for context on using zlib.
Status Uncompress(Slice input, ostream* out) {
  DCHECK(out);
  // Output stream exceptions aren't handled in this function, so the stream
  // isn't supposed to throw on errors, but just set bad/failure bits instead.
  DCHECK_EQ(ios::goodbit, out->exceptions());

  // Initialize the z_stream at the start of the data with the
  // data size as the available input.
  z_stream zs;
  memset(&zs, 0, sizeof(zs));
  zs.next_in = const_cast<uint8_t*>(input.data());
  zs.avail_in = input.size();
  // Initialize inflation with the windowBits set to be GZIP compatible.
  // The documentation (https://www.zlib.net/manual.html#Advanced) describes that
  // Adding 16 configures inflate to decode the gzip format.
  ZRETURN_NOT_OK(inflateInit2(&zs, MAX_WBITS + 16 /* enable gzip */));
  // Continue calling inflate, decompressing data into the buffer in
  // `zs.next_out` and writing the buffer content to `out`, until an error
  // condition is encountered or there is no more data to decompress.
  constexpr const size_t kChunkSize = 16 * 1024;
  unsigned char buf[kChunkSize];
  int rc;
  do {
    zs.next_out = buf;
    zs.avail_out = kChunkSize;
    rc = inflate(&zs, Z_NO_FLUSH);
    if (PREDICT_FALSE(rc != Z_OK && rc != Z_STREAM_END)) {
      ignore_result(inflateEnd(&zs));
      // Special handling for Z_BUF_ERROR in certain conditions to produce
      // Status::Corruption() instead of Status::RuntimeError().
      if (rc == Z_BUF_ERROR && zs.avail_in == 0) {
        // This means the input was most likely incomplete/truncated. Because
        // the input isn't a stream in this function, all the input data is
        // available at once as Slice, so no more available bytes on input
        // are ever expected at this point.
        return Status::Corruption("truncated gzip data");
      }
      return ZlibResultToStatus(rc);
    }
    DCHECK_GT(zs.next_out - buf, 0);
    out->write(reinterpret_cast<char*>(buf), zs.next_out - buf);
    if (PREDICT_FALSE(out->fail())) {
      ignore_result(inflateEnd(&zs));
      return Status::IOError("error writing to output stream");
    }
  } while (rc != Z_STREAM_END);

  // If we haven't returned early with a bad status, finalize inflation.
  return ZlibResultToStatus(inflateEnd(&zs));
}

} // namespace zlib
} // namespace kudu
