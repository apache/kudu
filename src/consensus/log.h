// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_LOG_H_
#define KUDU_CONSENSUS_LOG_H_

#include <string>
#include <vector>

#include "consensus/log_util.h"

namespace kudu {

class FsManager;

namespace log {

class LogReader;

// Log interface, inspired by Raft's (logcabin) Log. Provides durability to
// Kudu as a normal Write Ahead Log and also plays the role of persistent
// storage for the consensus state machine.
//
// Note: This class is not thread safe, the caller is expected to synchronize
// Log::Append() calls.
class Log {
 public:

  // Opens or continues a log and sets 'log' to the newly built Log.
  // After Open() the Log is ready to receive entries.
  // The passed 'super_block' and 'current_id' are copied.
  static Status Open(const LogOptions &options,
                     FsManager *fs_manager,
                     const metadata::TabletSuperBlockPB& super_block,
                     const consensus::OpId& current_id,
                     gscoped_ptr<Log> *log);

  // Append a new entry to the log.
  // Log does not take ownership of the passed 'entry'.
  Status Append(const LogEntry& entry);

  // Make segments roll over
  Status RollOver();

  // Syncs all state and closes the log.
  Status Close();

  void SetMaxSegmentSizeForTests(uint64_t max_segment_size) {
    max_segment_size_ = max_segment_size;
  }

  // Returns the current header so that the log can be queried for the most
  // current config/metadata/<term,index>.
  const LogSegmentHeader* current_header() const {
    return next_segment_header_.get();
  }

  // Runs the garbage collector on the set of previous segments. Segments that
  // only refer to in-mem state that has been flushed are candidates for
  // garbage collection.
  Status GC();

  const WritableLogSegment* current_segment() const {
    return current_.get();
  }

  const vector<std::tr1::shared_ptr<ReadableLogSegment> > &previous_segments() const {
    return previous_;
  }

  ~Log();

 private:
  DISALLOW_COPY_AND_ASSIGN(Log);

  Log(const LogOptions &options,
      FsManager *fs_manager,
      const string& log_path,
      gscoped_ptr<LogSegmentHeader> header);

  // Initializes a new one or continues an existing log.
  Status Init();

  // Creates the name for a new segment as log-<term>-<index>
  string CreateSegmentFileName(const consensus::OpId& id);

  // Creates a new segment and sets current_ to the new one.
  Status CreateNewSegment(const LogSegmentHeader& header);

  LogOptions options_;
  FsManager *fs_manager_;
  string log_dir_;

  // the next segment's header
  gscoped_ptr<LogSegmentHeader> next_segment_header_;
  // the current segment being written
  gscoped_ptr<WritableLogSegment> current_;
  // all previous, un-GC'd, segments
  vector<std::tr1::shared_ptr<ReadableLogSegment> > previous_;

  // the size of the header of the current segment
  uint64_t header_size_;
  // the maximum segment size, in bytes
  uint64_t max_segment_size_;

  // Reader for previous segments
  gscoped_ptr<LogReader> reader_;

  enum State {
    kLogInitialized,
    kLogWriting,
    kLogClosed
  };
  State state_;
};

}  // namespace log
}  // namespace kudu
#endif /* KUDU_CONSENSUS_LOG_H_ */
