// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_UTIL_OID_GENERATOR_H
#define KUDU_UTIL_OID_GENERATOR_H

#include <boost/uuid/uuid_generators.hpp>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/util/locks.h"

namespace kudu {

// Generates a unique 32byte id, based on uuid v4.
// This class is thread safe
class ObjectIdGenerator {
 public:
  ObjectIdGenerator() {}
  ~ObjectIdGenerator() {}

  std::string Next();

 private:
  DISALLOW_COPY_AND_ASSIGN(ObjectIdGenerator);

  typedef simple_spinlock LockType;

  LockType oid_lock_;
  boost::uuids::random_generator oid_generator_;
};

} // namespace kudu

#endif
