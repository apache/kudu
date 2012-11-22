// Copyright (c) 2012, Cloudera, inc.

#include <boost/noncopyable.hpp>
#include <tr1/memory>

#include "common/schema.h"
#include "tablet/deltamemstore.h"

namespace kudu {

class WritableFile;

namespace cfile {
class Writer;
}

namespace tablet {

using std::tr1::shared_ptr;

class DeltaFileWriter : boost::noncopyable {
public:
  explicit DeltaFileWriter(const Schema &schema,
                           const shared_ptr<WritableFile> &file);

  Status Start();
  Status Finish();

  Status AppendDelta(uint32_t row_idx, const RowDelta &delta);

private:
  const Schema schema_;

  scoped_ptr<cfile::Writer> writer_;

  // Buffer used as a temporary for storing the serialized form
  // of the deltas
  faststring tmp_buf_;

  #ifndef NDEBUG
  uint32_t last_row_idx_;
  #endif


};

} // namespace tablet
} // namespace kudu
