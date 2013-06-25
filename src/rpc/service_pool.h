// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_SERVICE_POOL_H
#define KUDU_SERVICE_POOL_H

#include <boost/thread.hpp>
#include <tr1/memory>
#include <vector>

#include "gutil/macros.h"
#include "gutil/gscoped_ptr.h"
#include "util/status.h"

namespace kudu {
namespace rpc {

class Messenger;
class ServiceIf;
class Socket;

// A pool of threads that handle new incoming RPC calls.
class ServicePool {
public:
  ServicePool(const std::tr1::shared_ptr<Messenger> &messenger,
              gscoped_ptr<ServiceIf> service);
  ~ServicePool();
  Status Init(int num_threads);

private:
  void RunThread();
  std::tr1::shared_ptr<Messenger> messenger_;
  gscoped_ptr<ServiceIf> service_;
  std::vector<std::tr1::shared_ptr<boost::thread> > threads_;

  DISALLOW_COPY_AND_ASSIGN(ServicePool);
};

} // namespace rpc
} // namespace kudu

#endif
