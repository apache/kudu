// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_LOG_LOG_READER_H_
#define KUDU_LOG_LOG_READER_H_

#include <string>
#include <vector>

#include "consensus/log_util.h"
#include "server/fsmanager.h"

namespace kudu {
namespace log {

// Reads a set of segments from a given path. Segment headers are read
// and parsed, but entries are not. In order to read log entries call
// LogReader::ReadEntries().
class LogReader {
 public:
  // Opens a LogReader on the default tablet log directory, and sets
  // 'reader' to the newly created LogReader.
  static Status Open(FsManager *fs_manager,
                     const string& tablet_oid,
                     gscoped_ptr<LogReader> *reader);

  // Opens a LogReader on a specific tablet log recovery directory, and sets
  // 'reader' to the newly created LogReader.
  static Status OpenFromRecoveryDir(FsManager *fs_manager,
                                    const string& tablet_oid,
                                    gscoped_ptr<LogReader> *reader);

  // Returns the number of segments in path_, set only once on Init().
  const uint32_t size();

  const vector<std::tr1::shared_ptr<ReadableLogSegment> > &segments() {
    DCHECK_EQ(state_ , kLogReaderReading) << "log reader was not Init()ed:"
                                          << state_;
    return segments_;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(LogReader);

  LogReader(FsManager *fs_manager,
            const string& tablet_name);

  // Reads the headers of all segments in 'path_'.
  Status Init(const string& path_);

  FsManager *fs_manager_;
  const string tablet_oid_;
  vector<std::tr1::shared_ptr<ReadableLogSegment> > segments_;

  enum State {
    kLogReaderInitialized,
    kLogReaderReading,
    kLogReaderClosed
  };

  State state_;
};

}  // namespace log
}  // namespace kudu

#endif /* KUDU_LOG_LOG_READER_H_ */
