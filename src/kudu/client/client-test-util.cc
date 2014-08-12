// Copyright (c) 2014, Cloudera, inc.

#include "kudu/client/client-test-util.h"

#include <boost/foreach.hpp>
#include <vector>

#include "kudu/gutil/stl_util.h"

namespace kudu {
namespace client {

void LogSessionErrorsAndDie(const std::tr1::shared_ptr<KuduSession>& session,
                            const Status& s) {
  CHECK(!s.ok());
  std::vector<KuduError*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  CHECK(!overflow);
  BOOST_FOREACH(const KuduError* e, errors) {
    LOG(INFO) << "Op " << e->failed_op().ToString()
              << " had status " << e->status().ToString();
  }
  CHECK_OK(s); // will fail
}

} // namespace client
} // namespace kudu
