// Copyright (c) 2012, Cloudera, inc.
//
// A Layer is a horizontal slice of a Kudu tablet.
// Each Layer contains data for a a disjoint set of keys.
// See src/tablet/README for a detailed description.

#ifndef KUDU_TABLET_LAYER_H
#define KUDU_TABLET_LAYER_H

#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <gtest/gtest.h>
#include <string>
#include <memory>

#include "cfile/cfile.h"
#include "cfile/cfile_reader.h"
#include "common/row.h"
#include "common/rowblock.h"
#include "common/schema.h"
#include "tablet/deltafile.h"
#include "tablet/deltamemstore.h"
#include "tablet/delta_tracker.h"
#include "tablet/layer-basedata.h"
#include "util/bloom_filter.h"
#include "util/memory/arena.h"

namespace kudu {

class Env;

namespace cfile {
class BloomFileWriter;
}

namespace tablet {

using boost::ptr_vector;
using std::string;
using std::auto_ptr;
using kudu::cfile::BloomFileWriter;
using kudu::cfile::CFileIterator;
using kudu::cfile::CFileReader;

class LayerWriter : boost::noncopyable {
public:
  LayerWriter(Env *env,
              const Schema &schema,
              const string &layer_dir,
              const BloomFilterSizing &bloom_sizing) :
    env_(env),
    schema_(schema),
    dir_(layer_dir),
    bloom_sizing_(bloom_sizing),
    finished_(false),
    column_flushed_counts_(schema.num_columns(), 0)
  {}

  Status Open();

  // TODO: doc me
  //
  // need_arena: if true, then the input iterators do not maintain
  // stable copies of all indirect data, a local arena is needed to hold
  // tmp copies during the flush.
  Status FlushProjection(const Schema &projection,
                         RowIteratorInterface *src_iter,
                         bool need_arena,
                         bool write_bloom);

  // TODO: this is only used by tests. Kill this off.
  Status WriteRow(const Slice &row) {
    CHECK(!finished_);
    DCHECK_EQ(row.size(), schema_.byte_size());

    for (int i = 0; i < schema_.num_columns(); i++) {
      int off = schema_.column_offset(i);
      const void *p = row.data() + off;
      RETURN_NOT_OK( cfile_writers_[i].AppendEntries(p, 1, 0) );

      column_flushed_counts_[i]++;
    }

    return Status::OK();
  }

  Status Finish();

  size_t written_count() const {
    CHECK(finished_);
    return column_flushed_counts_[0];
  }


private:

  Status InitBloomFileWriter(scoped_ptr<BloomFileWriter> *bfw) const;

  Env *env_;
  const Schema schema_;
  const string dir_;
  BloomFilterSizing bloom_sizing_;

  bool finished_;
  ptr_vector<cfile::Writer> cfile_writers_;
  vector<size_t> column_flushed_counts_;
};

////////////////////////////////////////////////////////////
// Layer
////////////////////////////////////////////////////////////

class Layer : public LayerInterface, boost::noncopyable {
public:
  static const char *kDeltaPrefix;
  static const char *kColumnPrefix;
  static const char *kBloomFileName;
  static const char *kTmpLayerSuffix;

  // Open a layer from disk.
  // If successful, sets *layer to the newly open layer
  static Status Open(Env *env,
                     const Schema &schema,
                     const string &layer_dir,
                     shared_ptr<Layer> *layer);

  // TODO: docme
  static Status CreatePartiallyFlushed(
    Env *env,
    const Schema &schema,
    const string &layer_dir,
    shared_ptr<MemStore> &memstore,
    shared_ptr<Layer> *new_layer);

  ////////////////////////////////////////////////////////////
  // "Management" functions
  ////////////////////////////////////////////////////////////

  // Flush all accumulated delta data to disk.
  Status FlushDeltas();

  // Delete the layer directory.
  Status Delete();

  // If the layer was created as "partially flushed" layer, finish the flush.
  // This renames the layer directory from its ".tmp" suffix to its final
  // location, and re-opens the base data cfiles.
  Status FinishFlush();

  ////////////////////////////////////////////////////////////
  // LayerInterface implementation
  ////////////////////////////////////////////////////////////

  ////////////////////
  // Updates
  ////////////////////
  Status UpdateRow(const void *key,
                   const RowDelta &update);

  Status CheckRowPresent(const LayerKeyProbe &probe, bool *present) const;

  ////////////////////
  // Read functions.
  ////////////////////
  RowIteratorInterface *NewRowIterator(const Schema &projection) const;


  // Count the number of rows in this layer.
  Status CountRows(size_t *count) const;

  // Estimate the number of bytes on-disk
  uint64_t EstimateOnDiskSize() const;

  boost::mutex *compact_flush_lock() {
    return &compact_flush_lock_;
  }

  const Schema &schema() const {
    return schema_;
  }

  string ToString() const {
    return dir_;
  }

  static string GetColumnPath(const string &dir, int col_idx);
  static string GetDeltaPath(const string &dir, int delta_idx);
  static string GetBloomPath(const string &dir);

private:
  FRIEND_TEST(TestLayer, TestLayerUpdate);
  FRIEND_TEST(TestLayer, TestDMSFlush);

  // TODO: should 'schema' be stored with the layer? quite likely
  // so that we can support cheap alter table.
  Layer(Env *env,
        const Schema &schema,
        const string &layer_dir) :
    env_(env),
    schema_(schema),
    dir_(layer_dir),
    open_(false),
    delta_tracker_(new DeltaTracker(env, schema, layer_dir))
  {}

  Status OpenBaseCFileReaders();

  Env *env_;
  const Schema schema_;
  string dir_;

  bool open_;

  // Base data for this layer.
  // This vector contains one entry for each column.
  shared_ptr<LayerBaseData> base_data_;
  shared_ptr<DeltaTracker> delta_tracker_;

  // Lock governing this layer's inclusion in a compact/flush. If locked,
  // no other compactor will attempt to include this layer.
  boost::mutex compact_flush_lock_;
};


} // namespace tablet
} // namespace kudu

#endif
