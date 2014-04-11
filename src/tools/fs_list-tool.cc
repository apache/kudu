// Copyright (c) 2014, Cloudera, inc.
//
// Tool to list local files and directories

#include "tools/fs_tool.h"

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <tr1/memory>
#include <iostream>
#include <vector>

#include "util/logging.h"

DEFINE_string(base_dir, "/tmp/demo-tablets", "Base directory for local files");
DEFINE_bool(verbose, false, "Print additional information (e.g., log segment headers)");

namespace kudu {
namespace tools {

using std::string;
using std::vector;

namespace {

enum CommandType {
  FS_TREE = 1,
  LIST_LOGS = 2,
  LIST_TABLETS = 3,
  LIST_BLOCKS = 4
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
    (CommandHandler(FS_TREE, "tree", "Print out a file system tree." ))
    (CommandHandler(LIST_LOGS, "list_logs",
                      "List file system logs (optionally accepts a tablet id)."))
    (CommandHandler(LIST_TABLETS, "list_tablets", "List tablets." ))
    (CommandHandler(LIST_BLOCKS, "list_blocks",
                    "List block for tablet (optionally accepts a tablet id)."));

void Usage(const string& prog_name, const string& msg) {
  std::cerr << "Error " << prog_name << ": " << msg << std::endl
            << std::endl
            << "Usage: " << prog_name << " [-verbose] -base_dir <dir> <command> [option] "
            << std::endl << std::endl
            << "Commands: " << std::endl;
  BOOST_FOREACH(const CommandHandler& handler, kCommandHandlers) {
    std::cerr << handler.name_ << ": " << handler.desc_ << std::endl;
  }
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
  Usage("Invalid command specified ", argv[1]);
  return false;
}

} // anonymous namespace

static int FsListToolMain(int argc, char** argv) {
  FLAGS_logtostderr = 1;
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
                 FLAGS_verbose ? FsTool::HEADERS_ONLY : FsTool::MINIMUM);
  CHECK_OK_PREPEND(fs_tool.Init(),
                   "Error initializing file system tool for in: " + FLAGS_base_dir);

  switch (cmd) {
    case FS_TREE: {
      CHECK_OK(fs_tool.FsTree());
      break;
    }
    case LIST_LOGS: {
      if (argc > 2) {
        CHECK_OK(fs_tool.ListLogSegmentsForTablet(argv[2]));
      } else {
        CHECK_OK(fs_tool.ListAllLogSegments());
      }
      break;
    }
    case LIST_TABLETS: {
      CHECK_OK(fs_tool.ListAllTablets());
      break;
    }
    case LIST_BLOCKS: {
      if (argc > 2) {
        CHECK_OK(fs_tool.ListBlocksForTablet(argv[2]));
      } else {
         CHECK_OK(fs_tool.ListBlocksForAllTablets());
      }
    }
  }

  return 0;
}

} // namespace tools
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::tools::FsListToolMain(argc, argv);
}
