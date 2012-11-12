// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_H
#define KUDU_TABLET_TABLET_H

#include <boost/scoped_ptr.hpp>
#include <string>

#include "tablet/memstore.h"
#include "tablet/schema.h"
#include "util/env.h"
#include "util/status.h"
#include "util/slice.h"

namespace kudu { namespace tablet {

using boost::scoped_ptr;
using std::string;

class Tablet {
public:
  Tablet(const Schema &schema,
         const string &dir);

  // Create a new tablet.
  // This will create the directory for this tablet.
  // After the call, the tablet may be opened with Open().
  // If the directory already exists, returns an IOError
  // Status.
  Status CreateNew();

  // Open an existing tablet.
  Status Open();

  // Insert a new row into the tablet.
  //
  // The provided 'data' slice should have length equivalent to this
  // tablet's Schema.byte_size().
  //
  // After insert, the row and any referred-to memory (eg for strings)
  // have been copied into internal memory, and thus the provided memory
  // buffer may safely be re-used or freed.
  //
  // Returns Status::OK unless allocation fails.
  Status Insert(const Slice &data);

  Status Flush();

private:
  Schema schema_;
  string dir_;
  scoped_ptr<MemStore> memstore_;

  Env *env_;

  bool open_;
};

} // namespace table
} // namespace kudu

#endif
