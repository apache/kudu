// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/server/server_base_options.h"

#include <gflags/gflags.h>
#include "kudu/util/flag_tags.h"

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
TAG_FLAG(server_dump_info_path, hidden);
TAG_FLAG(server_dump_info_format, hidden);

DEFINE_int32(metrics_log_interval_ms, 0,
             "Interval (in milliseconds) at which the server will dump its "
             "metrics to a local log file. The log files are located in the same "
             "directory as specified by the -log_dir flag. If this is not a positive "
             "value, then metrics logging will be disabled.");
TAG_FLAG(metrics_log_interval_ms, advanced);

ServerBaseOptions::ServerBaseOptions()
  : env(Env::Default()),
    dump_info_path(FLAGS_server_dump_info_path),
    dump_info_format(FLAGS_server_dump_info_format),
    metrics_log_interval_ms(FLAGS_metrics_log_interval_ms) {
}

} // namespace server
} // namespace kudu
