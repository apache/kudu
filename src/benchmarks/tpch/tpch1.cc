// Copyright (c) 2012, Cloudera, inc.
//
// This utility will first try to load the data from the given path if the
// tablet doesn't already exist at the given location. It will then run
// the tpch1 query, as described below, up to tpch_num_query_iterations times.
//
// The input data must be in the tpch format, separated by "|".
//
// Usage:
//   tpch1 -tpch_path_to_data=/home/jdcryans/lineitem.tbl
//         -tpch_path_to_tablet=/tmp/tpch1-test/tablet/
//         -tpch_num_query_iterations=1
//         -tpch_expected_matching_rows=12345
//
// From Impala:
// ====
// ---- QUERY : TPCH-Q1
// # Q1 - Pricing Summary Report Query
// # Modifications: Remove ORDER BY, added ROUND() calls
// select
//   l_returnflag,
//   l_linestatus,
//   round(sum(l_quantity), 1),
//   round(sum(l_extendedprice), 1),
//   round(sum(l_extendedprice * (1 - l_discount)), 1),
//   round(sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)), 1),
//   round(avg(l_quantity), 1),
//   round(avg(l_extendedprice), 1),
//   round(avg(l_discount), 1), count(1)
// from
//   lineitem
// where
//   l_shipdate<='1998-09-02'
// group by
//   l_returnflag,
//   l_linestatus
// ---- TYPES
// string, string, double, double, double, double, double, double, double, bigint
// ---- RESULTS
// 'A','F',37734107,56586554400.7,53758257134.9,55909065222.8,25.5,38273.1,0,1478493
// 'N','F',991417,1487504710.4,1413082168.1,1469649223.2,25.5,38284.5,0.1,38854
// 'N','O',74476040,111701729697.7,106118230307.6,110367043872.5,25.5,38249.1,0,2920374
// 'R','F',37719753,56568041380.9,53741292684.6,55889619119.8,25.5,38250.9,0.1,1478870
// ====
#include <stdio.h>
#include <stdlib.h>

#include <boost/assign/list_of.hpp>
#include <boost/tokenizer.hpp>
#include <glog/logging.h>

#include <iostream>
#include <fstream>
#include <map>

#include "common/scan_predicate.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "common/row.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/hash/city.h"
#include "gutil/strings/numbers.h"
#include "util/slice.h"
#include "util/stopwatch.h"
#include "tablet/tablet.h"


DEFINE_string(tpch_path_to_data, "/tmp/lineitem.tbl",
              "The full path to the '|' separated file containing the lineitem table.");
DEFINE_string(tpch_path_to_tablet, "/tmp/tpch", "The full path to the tablet's directory.");
DEFINE_int32(tpch_num_query_iterations, 1, "Number of times the query will be run.");
DEFINE_int32(tpch_expected_matching_rows, 5916591, "Number of rows that should match the query.");

namespace kudu {

typedef boost::tokenizer<boost::char_separator<char> > Tokenizer;

struct Result {
  int l_quantity;
  int l_extendedprice;
  int l_discount;
  int l_tax;
  int count;
  Result()
    : l_quantity(0), l_extendedprice(0), l_discount(0), l_tax(0), count(0) {
  }
};

// This struct is used for the keys while running the GROUP BY instead of manipulating strings
struct SliceMapKey {
  Slice slice;
  explicit SliceMapKey(const Slice &sl)
    : slice(sl) {
  }

  // This copies the string out of the result buffer
  void RelocateSlice() {
    size_t size = slice.size();
    uint8_t *buf = new uint8_t[size];
    memcpy(buf, slice.data(), size);
    slice = Slice(buf, size);
  }

  bool operator==(const SliceMapKey &other_key) const {
    return slice == other_key.slice;
  }
};

struct hash {
  size_t operator()(const SliceMapKey &key) const {
    return util_hash::CityHash64(
      reinterpret_cast<const char *>(key.slice.data()), key.slice.size());
  }
};

static const boost::char_separator<char> kPipeSeparator("|");

static const Schema kSchema(boost::assign::list_of
                            (ColumnSchema("l_orderkey_l_linenumber", STRING)) // no composite key
                            (ColumnSchema("l_partkey", UINT32))
                            (ColumnSchema("l_suppkey", UINT32))
                            (ColumnSchema("l_quantity", UINT32)) // decimal??
                            (ColumnSchema("l_extendedprice", UINT32)) // storing * 100
                            (ColumnSchema("l_discount", UINT32)) // storing * 100
                            (ColumnSchema("l_tax", UINT32)) // storing * 100
                            (ColumnSchema("l_returnflag", STRING))
                            (ColumnSchema("l_linestatus", STRING))
                            (ColumnSchema("l_shipdate", STRING, true))
                            (ColumnSchema("l_commitdate", STRING))
                            (ColumnSchema("l_receiptdate", STRING))
                            (ColumnSchema("l_shipinstruct", STRING))
                            (ColumnSchema("l_shipmode", STRING))
                            (ColumnSchema("l_comment", STRING))
                            , 1);

const Schema kQuerySchema(boost::assign::list_of
                            (ColumnSchema("l_shipdate", STRING, true))
                            (ColumnSchema("l_returnflag", STRING))
                            (ColumnSchema("l_linestatus", STRING))
                            (ColumnSchema("l_quantity", UINT32))
                            (ColumnSchema("l_extendedprice", UINT32))
                            (ColumnSchema("l_discount", UINT32))
                            (ColumnSchema("l_tax", UINT32))
                            , 0);

// returns true if the tablet already contains data
bool OpenTablet(const string &path, gscoped_ptr<tablet::Tablet> &tablet) {
  string tablet_dir(path);
  tablet.reset(new tablet::Tablet(kSchema, tablet_dir));
  tablet->CreateNew();
  CHECK_OK(tablet->Open());
  return tablet->num_rowsets() == 0;
}

void ConvertToIntAndPopulate(const string &chars, RowBuilder *rb) {
  int number;
  bool ok_parse = SimpleAtoi(chars.c_str(), &number);
  CHECK(ok_parse);
  rb->AddUint32(number);
}

void ConvertDoubleToIntAndPopulate(const string &chars, RowBuilder *rb) {
  char *error = NULL;
  errno = 0;
  const char *cstr = chars.c_str();
  double number = strtod(cstr, &error);
  CHECK(errno == 0 &&  // overflow/underflow happened
      error != cstr);
  int new_num = number * 100;
  rb->AddUint32(new_num);
}

void LoadLineItems(const string &path, gscoped_ptr<tablet::Tablet> &tablet) {
  std::ifstream in(path.c_str());
  CHECK(in.is_open()) << "not able to open input file: " << path;

  string line;
  RowBuilder rb(kSchema);
  vector<string> columns;

  while (getline(in,line)) {
    columns.clear();
    rb.Reset();
    Tokenizer tokens(line, kPipeSeparator);

    // grab all the columns individually
    columns.assign(tokens.begin(), tokens.end());

    char buf[256];
    snprintf(buf, sizeof(buf), "%s_%s", columns[0].c_str(), columns[3].c_str());
    rb.AddString(Slice(buf));

    ConvertToIntAndPopulate(columns[1], &rb);
    ConvertToIntAndPopulate(columns[2], &rb);
    ConvertToIntAndPopulate(columns[4], &rb);
    ConvertDoubleToIntAndPopulate(columns[5], &rb);
    ConvertDoubleToIntAndPopulate(columns[6], &rb);
    ConvertDoubleToIntAndPopulate(columns[7], &rb);
    for (int i = 8; i < 16; i++)  {
      rb.AddString(Slice(columns[i]));
    }

    CHECK_OK(tablet->Insert(rb.row()));
  }
  CHECK_OK(tablet->Flush());
}

// this function encapsulates the ugliness of efficiently getting the value
template <class extract_type>
const extract_type * ExtractColumn(RowBlock &block, int row, int column) {
  return reinterpret_cast<const extract_type *>(block.column_block(column).cell_ptr(row));
}

void Tpch1(gscoped_ptr<tablet::Tablet> &tablet) {
  typedef unordered_map<SliceMapKey, Result*, hash> slice_map;
  typedef unordered_map<SliceMapKey, slice_map*, hash> slice_map_map;

  gscoped_ptr<RowwiseIterator> iter;
  CHECK_OK(tablet->NewRowIterator(kQuerySchema, &iter));
  Slice date("1998-09-02");
  ScanSpec spec;
  ColumnRangePredicate pred1(kQuerySchema.column(0), boost::none, &date);
  spec.AddPredicate(pred1);
  CHECK_OK(iter->Init(&spec));

  Arena arena(32*1024, 256*1024);
  RowBlock block(kQuerySchema, 1000, &arena);
  int matching_rows = 0;
  slice_map_map results;
  Result *r;
  while (iter->HasNext()) {
    CHECK_OK(RowwiseIterator::CopyBlock(iter.get(), &block));
    RowBlockRow rb_row;
    for (int i = 0; i < block.nrows(); i++) {
      if (!block.selection_vector()->IsRowSelected(i)) continue;
      rb_row = block.row(i);
      matching_rows++;

      SliceMapKey l_returnflag(*ExtractColumn<Slice>(block, i, 1));
      SliceMapKey l_linestatus(*ExtractColumn<Slice>(block, i, 2));

      int l_quantity = *ExtractColumn<uint32_t>(block, i, 3);
      int l_extendedprice = *ExtractColumn<uint32_t>(block, i, 4);
      int l_discount = *ExtractColumn<uint32_t>(block, i, 5);
      int l_tax = *ExtractColumn<uint32_t>(block, i, 6);

      slice_map *linestatus_map;
      slice_map_map::iterator it = results.find(l_returnflag);
      if (it == results.end()) {
        linestatus_map = new slice_map;
        l_returnflag.RelocateSlice();
        results[l_returnflag] = linestatus_map;
      } else {
        linestatus_map = it->second;
      }

      slice_map::iterator inner_it = linestatus_map->find(l_linestatus);
      if (inner_it == linestatus_map->end()) {
        r = new Result();
        l_linestatus.RelocateSlice();
        (*linestatus_map)[l_linestatus] = r;
      } else {
        r = inner_it->second;
      }
      r->l_quantity += l_quantity;
      r->l_extendedprice += l_extendedprice;
      r->l_discount += l_discount;
      r->l_tax += l_tax;
      r->count++;
    }
  }
  LOG(INFO) << "Result: ";
  for (slice_map_map::iterator ii = results.begin();
       ii != results.end(); ++ii) {
    const SliceMapKey returnflag = ii->first;
    slice_map *maps = ii->second;
    for (slice_map::iterator jj = maps->begin(); jj != maps->end(); ++jj) {
      const SliceMapKey linestatus = jj->first;
      Result *r = jj->second;
      double avg_q = static_cast<double>(r->l_quantity) / r->count;
      double avg_ext_p = r->l_extendedprice / r->count / 100.0;
      double avg_discount = r->l_discount / r->count / 100.0;
      LOG(INFO) << returnflag.slice.ToString() << ", " <<
                   linestatus.slice.ToString() << ", " <<
                   r->l_quantity << ", " <<
                   (r->l_extendedprice / 100.0) << ", " <<
                   // TODO those two are missing at the moment, might want to chagne Result
                   // sum(l_extendedprice * (1 - l_discount))
                   // sum(l_extendedprice * (1 - l_discount) * (1 + l_tax))
                   avg_q << ", " <<
                   avg_ext_p << ", " <<
                   avg_discount << ", " <<
                   r->count;
      delete r;
      delete linestatus.slice.data();
    }
    delete maps;
    delete returnflag.slice.data();
  }
  CHECK_EQ(matching_rows, FLAGS_tpch_expected_matching_rows) << "Wrong number of rows returned";
}

} // namespace kudu

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  gscoped_ptr<kudu::tablet::Tablet> tablet;
  bool needs_loading = kudu::OpenTablet(FLAGS_tpch_path_to_tablet, tablet);
  if (needs_loading) {
    LOG_TIMING(INFO, "loading") {
      kudu::LoadLineItems(FLAGS_tpch_path_to_data, tablet);
    }
  } else {
    LOG(INFO) << "Data already in place";
  }
  for (int i = 0; i < FLAGS_tpch_num_query_iterations; i++) {
    LOG_TIMING(INFO, StringPrintf("querying for iteration # %d", i)) {
      kudu::Tpch1(tablet);
    }
  }

  return 0;
}
