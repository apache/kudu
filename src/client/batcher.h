// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CLIENT_BATCHER_H
#define KUDU_CLIENT_BATCHER_H

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "util/status.h"
#include <vector>

namespace kudu {
namespace client {

class Insert;
class RemoteTabletServer;

struct InFlightOp;
struct PerTSBuffer;

class Batcher {
 public:
  Batcher();
  virtual ~Batcher();

  Status Add(gscoped_ptr<Insert> insert);

 private:
  size_t mem_usage_;
  std::vector<InFlightOp*> ops_;

  unordered_map<RemoteTabletServer*, PerTSBuffer*> per_ts_buffers_;

  DISALLOW_COPY_AND_ASSIGN(Batcher);
};

} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_BATCHER_H */
