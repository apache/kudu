// Copyright (c) 2014, Cloudera,inc.

#include "client/session-internal.h"

#include <boost/thread/locks.hpp>

#include "client/batcher.h"
#include "client/error_collector.h"

using std::tr1::shared_ptr;

namespace kudu {

namespace client {

using internal::Batcher;
using internal::ErrorCollector;

KuduSession::Data::Data(const shared_ptr<KuduClient>& client)
  : client_(client),
    error_collector_(new ErrorCollector()),
    flush_mode_(AUTO_FLUSH_SYNC),
    timeout_ms_(-1) {}

KuduSession::Data::~Data() {
}

void KuduSession::Data::Init(const shared_ptr<KuduSession>& session) {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK(!batcher_);
  NewBatcher(session, NULL);
}

void KuduSession::Data::NewBatcher(const shared_ptr<KuduSession>& session,
                                   scoped_refptr<Batcher>* old_batcher) {
  DCHECK(lock_.is_locked());

  scoped_refptr<Batcher> batcher(
    new Batcher(client_.get(), error_collector_.get(), session));
  if (timeout_ms_ != -1) {
    batcher->SetTimeoutMillis(timeout_ms_);
  }
  batcher.swap(batcher_);

  if (old_batcher) {
    old_batcher->swap(batcher);
  }
}

void KuduSession::Data::FlushFinished(Batcher* batcher) {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(flushed_batchers_.erase(batcher), 1);
}

} // namespace client
} // namespace kudu
