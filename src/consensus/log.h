// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_LOG_H_
#define KUDU_CONSENSUS_LOG_H_

#include <string>
#include <vector>

#include "consensus/log_util.h"
#include "util/task_executor.h"

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

  // This class represents a batch of operations to be written and
  // synced to the log. It is opaque to the user and is managed by the
  // Log class.
  class Entry {
   public:
    ~Entry();

   private:
    friend class Log;

    Entry(gscoped_ptr<LogEntryPB[]> phys_entries, size_t count);

    void set_callback(const std::tr1::shared_ptr<FutureCallback>& cb) {
      callback_ = cb;
    }

    const std::tr1::shared_ptr<FutureCallback>& callback() {
      return callback_;
    }

    size_t count() const { return count_; }

    // Contents of the log entries that will be written to disk.
    //
    // Note: in the final version of group commit this will likely be
    // a pointer to storage managed by log itself.
    gscoped_ptr<LogEntryPB[]> phys_entries_;

    // Callback to be invoked upon the entries being written and
    // synced to disk.
    std::tr1::shared_ptr<FutureCallback> callback_;

    size_t count_;

    // Until group commit is implemented, most of the work that
    // *should* be done by AsyncAppend() is done by Reserve().
    // However, it makes that the callback should still be added
    // as a parameter to AsyncAppend(). To accomodate for this,
    // the future submitted to the executor in the (temprorary)
    // version of Reserve() is stored here so that AsyncAppend()
    // can add a callback.
    std::tr1::shared_ptr<kudu::Future> future_;

    DISALLOW_COPY_AND_ASSIGN(Entry);
  };

  // Opens or continues a log and sets 'log' to the newly built Log.
  // After Open() the Log is ready to receive entries.
  // The passed 'super_block' and 'current_id' are copied.
  static Status Open(const LogOptions &options,
                     FsManager *fs_manager,
                     const metadata::TabletSuperBlockPB& super_block,
                     const consensus::OpId& current_id,
                     gscoped_ptr<Log> *log);

  // [Once group commit is implemented, this method will] reserve a
  // spot in the log for operations in 'ops'; 'reserved_entry' is
  // initialized by this method and any resources associated with it
  // will be released in AsyncAppend().
  // In order to ensure correct ordering of operations across multiple
  // threads, calls to this method must be externall synchronized.
  //
  // NOTE: the caller _must_ call AsyncAppend() or else the log will
  // "stall" and will never be able to make forward progress.
  Status Reserve(const vector<consensus::OperationPB*>& ops,
                 Entry** reserved_entry);

  // Asynchronously appends 'entry' to the log. Once the append
  // completes and synced 'callback' will be invoked.
  Status AsyncAppend(Entry* entry,
                     const std::tr1::shared_ptr<FutureCallback>& callback);

  // Synchronously append a new entry to the log.
  // Log does not take ownership of the passed 'entry'.
  Status Append(LogEntryPB* entry);

  // Make segments roll over
  Status RollOver();

  // Syncs all state and closes the log.
  Status Close();

  void SetMaxSegmentSizeForTests(uint64_t max_segment_size) {
    max_segment_size_ = max_segment_size;
  }

  // Returns the current header so that the log can be queried for the most
  // current config/metadata/<term,index>.
  const LogSegmentHeaderPB* current_header() const {
    return next_segment_header_.get();
  }

  const consensus::OpId& last_entry_id() const {
    return next_segment_header_->initial_id();
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
      gscoped_ptr<LogSegmentHeaderPB> header);

  // Initializes a new one or continues an existing log.
  Status Init();

  // Creates the name for a new segment as log-<term>-<index>
  string CreateSegmentFileName(const consensus::OpId& id);

  // Creates a new segment and sets current_ to the new one.
  Status CreateNewSegment(const LogSegmentHeaderPB& header);

  LogOptions options_;
  FsManager *fs_manager_;
  string log_dir_;

  // the next segment's header
  gscoped_ptr<LogSegmentHeaderPB> next_segment_header_;
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

  // Single threaded executor for writing to the log
  gscoped_ptr<TaskExecutor> log_executor_;

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
