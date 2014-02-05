// Copyright (c) 2014, Cloudera, inc.

#include "server/server_base_options.h"

#include <gflags/gflags.h>

namespace kudu {
namespace server {

DEFINE_string(server_dump_info_path, "",
              "Path into which the server information will be "
              "dumped after startup. The dumped data is described by "
              "ServerStatusPB in server_base.proto. The dump format is "
              "determined by --server_dump_info_format");
DEFINE_string(server_dump_info_format, "json",
              "Format for --server_dump_info_path. This may be either "
              "'pb' or 'json'.");

ServerBaseOptions::ServerBaseOptions()
  : dump_info_path(FLAGS_server_dump_info_path),
    dump_info_format(FLAGS_server_dump_info_format) {
}

} // namespace server
} // namespace kudu
