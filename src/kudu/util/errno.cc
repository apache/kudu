//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
 * Get the POSIX strerror_r.
 *
 * By default, glibc uses its own, non-POSIX, definition of strerror_r.  We want
 * the POSIX version. 
 *
 * Note: This must come first!  If you include anything before this point, you
 * might get the non-standard strerror_r definition.
 */
#undef _GNU_SOURCE
#define _XOPEN_SOURCE 600
#include <string.h>
#define _GNU_SOURCE
#undef _XOPEN_SOURCE

namespace kudu {

void ErrnoToCString(int err, char *buf, size_t buf_len) {
  if (strerror_r(err, buf, buf_len)) {
    static const char UNKNOWN_ERROR[] = "unknown error";
    if (buf_len >= sizeof(UNKNOWN_ERROR)) {
      strcpy(buf, UNKNOWN_ERROR); // NOLINT(runtime/printf)
    } else {
      memset(buf, 0, buf_len);
    }
  }
}
} // namespace kudu
