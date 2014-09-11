// Copyright (c) 2014, Cloudera, inc.
//
// Tool to dump tablets, rowsets, and blocks

#include "kudu/tools/fs_tool.h"

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <sstream>
#include <tr1/memory>
#include <vector>

#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/logging.h"

DEFINE_string(base_dir, "/tmp/demo-tablets", "Base directory for local files");
DEFINE_int32(nrows, 0, "Number of rows to dump");

/*
  TODO: support specifying start and end keys

  DEFINE_string(start_key, "", "Start key for rows to dump");
  DEFINE_string(end_key, "", "Start key for rows to dump");
*/

DEFINE_bool(headers_only, false, "Don't dump contents, dump headers only");

namespace kudu {
namespace tools {

using std::string;
using std::vector;
using strings::Substitute;

namespace {

enum CommandType {
  DUMP_TABLET,
  DUMP_ROWSET,
  DUMP_CFILE_BLOCK,
  PRINT_TABLET_META,
};

struct CommandHandler {
  CommandType type_;
  string name_;
  string desc_;

  CommandHandler(CommandType type, const string& name, const string& desc)
      : type_(type),
        name_(name),
        desc_(desc) {
  }
};

const vector<CommandHandler> kCommandHandlers = boost::assign::list_of
    (CommandHandler(DUMP_TABLET, "dump_tablet", "Dump a tablet (requires a tablet id)"))
    (CommandHandler(DUMP_ROWSET, "dump_rowset",
                    "Dump a rowset (requires a tablet id and an index)"))
    (CommandHandler(DUMP_CFILE_BLOCK, "dump_block",
                    "Dump a cfile block (requires a block id"))
    (CommandHandler(PRINT_TABLET_META, "print_meta",
                    "Print a tablet metadata (requires a tablet id)"));

void PrintUsageToStream(const std::string& prog_name, std::ostream* out) {
  *out << "Usage: " << prog_name
       << " [-headers_only] [-nrows <num rows>] -base_dir <dir> <command> <options> "
       << std::endl << std::endl;
  *out << "Commands: " << std::endl;
  BOOST_FOREACH(const CommandHandler& handler, kCommandHandlers) {
    *out << handler.name_ << ": " << handler.desc_ << std::endl;
  }
}
void Usage(const string& prog_name, const string& msg) {
  std::cerr << "Error " << prog_name << ": " << msg << std::endl;
  PrintUsageToStream(prog_name, &std::cerr);
}

bool ValidateCommand(int argc, char** argv, CommandType* out) {
  if (argc < 2) {
    Usage(argv[0], "At least one command must be specified!");
    return false;
  }
  BOOST_FOREACH(const CommandHandler& handler, kCommandHandlers) {
    if (argv[1] == handler.name_) {
      *out = handler.type_;
      return true;
    }
  }
  Usage("Invalid command specified: ", argv[1]);
  return false;
}

} // anonymous namespace

static int FsDumpToolMain(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  std::stringstream usage_str;
  PrintUsageToStream(argv[0], &usage_str);
  google::SetUsageMessage(usage_str.str());
  google::ParseCommandLineFlags(&argc, &argv, true);
  InitGoogleLoggingSafe(argv[0]);

  if (FLAGS_base_dir == "") {
    Usage(argv[0], "'-base_dir' is required");
    return 2;
  }

  CommandType cmd;
  if (!ValidateCommand(argc, argv, &cmd)) {
    return 2;
  }

  FsTool fs_tool(FLAGS_base_dir,
                 FLAGS_headers_only ? FsTool::HEADERS_ONLY : FsTool::MAXIMUM);
  CHECK_OK(fs_tool.Init());

  DumpOptions opts;
  // opts.start_key = FLAGS_start_key;
  // opts.end_key = FLAGS_end_key;
  opts.nrows = FLAGS_nrows;

  switch (cmd) {
    case DUMP_TABLET: {
      if (argc < 3) {
        Usage(argv[0],
              Substitute("dump_tablet requires tablet id: $0 "
                         " -base_dir /kudu dump_tablet <tablet_id>",
                         argv[0]));
        return 2;
      }
      CHECK_OK(fs_tool.DumpTablet(argv[2], opts, 0));
      break;
    }
    case DUMP_ROWSET: {
      if (argc < 4) {
        Usage(argv[0],
              Substitute("dump_rowset requires tablet id and rowset index: $0"
                         " -base_dir /kudu dump_rowset <tablet_id> <rowset_index>",
                         argv[0]));
        return 2;
      }
      uint32_t rowset_idx;
      CHECK(safe_strtou32(argv[3], &rowset_idx))
          << "Invalid index specified: " << argv[2];
      CHECK_OK(fs_tool.DumpRowSet(argv[2], rowset_idx, opts, 0));
      break;
    }
    case DUMP_CFILE_BLOCK: {
      if (argc < 3) {
        Usage(argv[0],
              Substitute("dump_block requires a block id: $0"
                         " -base_dir /kudu dump_block <block_id>", argv[0]));
        return 2;
      }
      CHECK_OK(fs_tool.DumpCFileBlock(argv[2], opts, 0));
      break;
    }
    case PRINT_TABLET_META: {
      if (argc < 3) {
        Usage(argv[0], Substitute("print_meta requires a tablet id: $0"
                                  " -base_dir /kudu print_meta <tablet_id>", argv[0]));
        return 2;
      }
      CHECK_OK(fs_tool.PrintTabletMeta(argv[2], 0));
      break;
    }
  }

  return 0;
}

} // namespace tools
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::tools::FsDumpToolMain(argc, argv);
}
