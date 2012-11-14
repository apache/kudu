// Copyright (c) 2012, Cloudera, inc.
//
// A Layer is a horizontal slice of a Kudu tablet.
// Each Layer contains data for a a disjoint set of keys.
// See src/tablet/README for a detailed description.

#ifndef KUDU_TABLET_LAYER_H
#define KUDU_TABLET_LAYER_H

#include <boost/ptr_container/ptr_vector.hpp>
#include <string>

#include "cfile/cfile.h"
#include "cfile/cfile_reader.h"
#include "tablet/schema.h"

namespace kudu {

class Env;

namespace tablet {

using boost::ptr_vector;
using std::string;

class LayerWriter : boost::noncopyable {
public:
  LayerWriter(Env *env,
              const Schema &schema,
              const string &layer_dir) :
    env_(env),
    schema_(schema),
    dir_(layer_dir)
  {}

  Status Open();

  Status WriteRow(const Slice &row) {
    DCHECK_EQ(row.size(), schema_.byte_size());

    for (int i = 0; i < schema_.num_columns(); i++) {
      int off = schema_.column_offset(i);
      const void *p = row.data() + off;
      RETURN_NOT_OK( cfile_writers_[i].AppendEntries(p, 1) );
    }

    return Status::OK();
  }

  Status Finish();

private:
  Env *env_;
  const Schema schema_;
  const string dir_;

  ptr_vector<cfile::Writer> cfile_writers_;
};


class LayerReader : boost::noncopyable {
public:

  // TODO: should 'schema' be stored with the layer? quite likely
  // so that we can support cheap alter table.
  LayerReader(Env *env,
              const Schema &schema,
              const string &layer_dir) :
    env_(env),
    schema_(schema),
    dir_(layer_dir),
    open_(false)
  {}


  Status Open();

private:
  Env *env_;
  const Schema schema_;
  const string dir_;

  bool open_;
  ptr_vector<cfile::CFileReader> cfile_readers_;
};

} // namespace tablet
} // namespace kudu

#endif
