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
  static Status Open(FsManager *fs_manager,
                     const string& tablet_oid,
                     uint64_t recovery_ts,
                     gscoped_ptr<LogReader> *reader);

  // Initializes a ReadableLogSegment based on a log file on the provided path.
  static Status InitSegment(Env* env,
                            const string &log_file,
                            shared_ptr<ReadableLogSegment>* segment);

  // Reads all entries of the provided segment, adds them the 'entries' vector.
  // The 'entries' vector owns the read entries.
  // If the log is corrupted (i.e. the returned 'Status' is 'Corruption') all
  // the log entries read up to the corrupted are returned in the 'entries' vector.
  static Status ReadEntries(
      const std::tr1::shared_ptr<ReadableLogSegment> &segment,
      vector<LogEntryPB* >* entries);

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

  // Parses the magic and header length and sets *parsed_len to the header size.
  static Status ParseMagicAndLength(const Slice &data,
                                    uint32_t *parsed_len);

  // Reads the magic and header length from the log segment.
  static Status ReadMagicAndHeaderLength(
      const std::tr1::shared_ptr<RandomAccessFile> &file,
      uint32_t *len);

  // Parses the header of the log segment, build a readable segment with that
  // header and sets segment to point to the new segment.
  static Status ParseHeaderAndBuildSegment(
      const uint64_t file_size,
      const string &path,
      const std::tr1::shared_ptr<RandomAccessFile> &file,
      std::tr1::shared_ptr<ReadableLogSegment> *segment);

  // Parses length of the log entry and sets *parsed_len to this value.
  static Status ParseEntryLength(const Slice &data,
                                 uint32_t *parsed_len);

  // Reads length from a segment and sets *len to this value.
  // Also increments the passed offset* by the length of the entry.
  static Status ReadEntryLength(
      const std::tr1::shared_ptr<ReadableLogSegment> &segment,
      uint64_t *offset,
      uint32_t *len);

  // Reads a log entry from the provided readable segment, which gets decoded
  // into 'entry' and increments 'offset' by the entry's length.
  static Status ReadEntry(
      const std::tr1::shared_ptr<ReadableLogSegment> &segment,
      faststring *tmp_buf,
      uint64_t *offset,
      uint32_t length,
      gscoped_ptr<LogEntryPB> *entry);

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
