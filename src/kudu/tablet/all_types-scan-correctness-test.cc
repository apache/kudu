#include <glog/logging.h>
#include <gtest/gtest.h>

#include <kudu/util/flags.h>

#include "kudu/common/schema.h"
#include "kudu/tablet/tablet-test-base.h"

namespace kudu {
namespace tablet {

struct RowOpsBase {
  RowOpsBase(DataType type, EncodingType encoding) {
    schema_ = Schema({ColumnSchema("key", INT32),
                     ColumnSchema("val_a", type, true, nullptr, nullptr,
                         ColumnStorageAttributes(encoding, DEFAULT_COMPRESSION)),
                     ColumnSchema("val_b", type, true, nullptr, nullptr,
                         ColumnStorageAttributes(encoding, DEFAULT_COMPRESSION))}, 1);

  }
  Schema schema_;
  Schema altered_schema_;
};

template<typename KeyTypeWrapper>
struct SliceTypeRowOps : public RowOpsBase {
  SliceTypeRowOps() : RowOpsBase(KeyTypeWrapper::type, KeyTypeWrapper::encoding),
    strs_(16), slices_(16), cur(0) {}

  // Assumes the string representation of n is under strlen characters.
  std::string LeftZeroPadded(const int& n, const int& strlen) {
    return StringPrintf(Substitute("%0$0$1", strlen, PRId64).c_str(), static_cast<int64_t>(n));
  }

  void GenerateRow(const int& value, KuduPartialRow* row) {
    if (value < 0) {
      CHECK_OK(row->SetNull(1));
      CHECK_OK(row->SetNull(2));
    } else {
      std::string string_a = LeftZeroPadded(value, 10);
      std::string string_b = LeftZeroPadded(value, 10);
      Slice slice_a(string_a);
      Slice slice_b(string_b);
      CHECK_OK(row->SetSliceCopy<TypeTraits<KeyTypeWrapper::type>>(1, slice_a));
      CHECK_OK(row->SetSliceCopy<TypeTraits<KeyTypeWrapper::type>>(2, slice_b));
    }
  }

  ColumnPredicate GenerateRangePredicate(const size_t& col, const int& lower, const int& upper) {
    // Key predicate strings and slices in scope in a vector.
    strs_[cur] = LeftZeroPadded(lower, 10);
    strs_[cur+1] = LeftZeroPadded(upper, 10);
    slices_[cur] = Slice(strs_[cur]);
    slices_[cur+1] = Slice(strs_[cur+1]);
    auto pred = ColumnPredicate::Range(schema_.column(col), &slices_[cur], &slices_[cur+1]);
    cur += 2;
    return pred;
  }

  // To avoid issues where either vector is resized, potentially disrupting the correspondence of
  // address/Slice to string, preallocate the sizes of these vectors as needed.
  std::vector<std::string> strs_;
  std::vector<Slice> slices_;
  size_t cur;
};

template<typename KeyTypeWrapper>
struct NumTypeRowOps : public RowOpsBase {
  NumTypeRowOps() : RowOpsBase(KeyTypeWrapper::type, KeyTypeWrapper::encoding),
    nums_(16), cur(0) {}

  typedef typename TypeTraits<KeyTypeWrapper::type>::cpp_type CppType;

  void GenerateRow(const int& value, KuduPartialRow* row) {
    if (value < 0) {
      CHECK_OK(row->SetNull(1));
      CHECK_OK(row->SetNull(2));
    } else {
      row->Set<TypeTraits<KeyTypeWrapper::type>>(1, value);
      row->Set<TypeTraits<KeyTypeWrapper::type>>(2, value);
    }
  }

  ColumnPredicate GenerateRangePredicate(const size_t& col , const int& lower, const int& upper) {
    nums_[cur] = lower;
    nums_[cur+1] = upper;
    auto pred = ColumnPredicate::Range(schema_.column(col), &nums_[cur], &nums_[cur+1]);
    cur += 2;
    return pred;
  }

  // To avoid issues where the vector is resized, potentially disrupting the correspondence of
  // address to value, preallocate the sizes of this vector as needed.
  std::vector<CppType> nums_;
  size_t cur;
};

// Calculates the number of values in the range [lower, upper) given a repeated, completely
// non-null pattern with the specified cardinality and the specified number of rows.
int ExpectedCount(const int& nrows, const int& cardinality, const int& lower_val,
    const int& upper_val) {
  if (lower_val >= upper_val || lower_val >= cardinality) {
    return 0;
  }
  int lower = std::max(0, lower_val);
  int upper = std::min(cardinality, upper_val);
  int last_chunk_count = 0;
  int last_chunk_size = nrows % cardinality;

  if (last_chunk_size == 0 || last_chunk_size <= lower) {
    // e.g. lower: 3, upper: 8, cardinality:10, nrows: 23, last_chunk_size: 3
    // Resulting vector: [0001111100 0001111100 000]
    last_chunk_count = 0;
  } else if (last_chunk_size <= upper) {
    // e.g. lower: 3, upper: 8, cardinality:10, nrows: 25, last_chunk_size: 5
    // Resulting vector: [0001111100 0001111100 00011]
    last_chunk_count = last_chunk_size - lower;
  } else {
    // e.g. lower: 3, upper: 8, cardinality:10, nrows: 29, last_chunk_size: 9
    // Resulting vector: [0001111100 0001111100 000111110]
    last_chunk_count = upper - lower;
  }
  return (nrows / cardinality) * (upper - lower) + last_chunk_count;
}

void ScopedTraceMsg(const std::string& test_name, const int& expected, const int& actual) {
  std::ostringstream ss;
  ss << test_name << " Scan Results: " << expected << " expected, " << actual << " returned.";
  SCOPED_TRACE(ss.str());
}

// Tests for correctness by running predicates on repeated patterns of rows, specified by nrows,
// cardinality, and null_upper. RowOps is a specialized RowOpsBase, defining how the tablet should
// add rows and generate predicates for various types.
template <class RowOps>
class AllTypesScanCorrectnessTest : public KuduTabletTest {
public:
  AllTypesScanCorrectnessTest() : KuduTabletTest(RowOps().schema_), rowops_(RowOps()),
      schema_(rowops_.schema_), base_nrows_(0), base_cardinality_(0), base_null_upper_(0) {}

  void SetUp() override {
    KuduTabletTest::SetUp();
  }

  // Adds a pattern of repeated values with the first "null_upper" of every "cardinality" rows
  // being set to null.
  // E.g. nrows: 9, cardinality: 5, null_upper: 2
  // [-, -, 2, 3, 4, -, -, 2, 3]
  void FillTestTablet(int nrows, int cardinality, int null_upper) {
    base_nrows_ = nrows;
    base_cardinality_ = cardinality;
    base_null_upper_ = null_upper;
    RowBuilder rb(client_schema_);
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);
    for (int i = 0; i < nrows; i++) {
      CHECK_OK(row.SetInt32(0, i));

      // Populate the bottom of the repeating pattern with NULLs.
      // Note: Non-positive null_upper will yield a completely non-NULL column.
      if (i % cardinality < null_upper) {
        rowops_.GenerateRow(-1, &row);
      } else {
        rowops_.GenerateRow(i % cardinality, &row);
      }
      ASSERT_OK_FAST(writer.Insert(row));
    }
    ASSERT_OK(tablet()->Flush());
  }

  // Scan the results of a query. Set "count" to the number of results satisfying the predicates.
  // ScanSpec must have all desired predicates already added to it.
  void ScanWithSpec(ScanSpec spec, int* count) {
    Arena arena(1028, 1028);
    AutoReleasePool pool;
    *count = 0;
    spec.OptimizeScan(client_schema_, &arena, &pool, true);
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(client_schema_, &iter));
    ASSERT_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicate.";
    ASSERT_OK(SilentIterateToStringList(iter.get(), count));
  }

  void RunTests() {
    int lower = 3;
    int upper = 10;
    int lower_non_null = 3;
    int zero = 0;
    if (lower < base_null_upper_) {
      lower_non_null = base_null_upper_;
    }

    int count = 0;
    {
      ScanSpec spec;
      auto pred = rowops_.GenerateRangePredicate(1, lower, upper);
      spec.AddPredicate(pred);
      ScanWithSpec(spec, &count);
      int expected_count = ExpectedCount(base_nrows_, base_cardinality_, lower_non_null, upper);
      ScopedTraceMsg("Range", expected_count, count);
      ASSERT_EQ(expected_count, count);
    }
    {
      // MultiColumn Range scan
      // This predicates two columns:
      //   col_a: [0, upper_val)
      //   col_b: [lower_val, cardinality)
      // Since the two columns have identical data, the result will be:
      //   AND:   [lower_val, upper_val)
      ScanSpec spec;
      ColumnPredicate pred_a = rowops_.GenerateRangePredicate(1, zero, upper);
      spec.AddPredicate(pred_a);
      ColumnPredicate pred_b = rowops_.GenerateRangePredicate(2, lower, base_cardinality_);
      spec.AddPredicate(pred_b);
      ScanWithSpec(spec, &count);
      int expected_count = ExpectedCount(base_nrows_, base_cardinality_, lower_non_null, upper);
      ScopedTraceMsg("MultiColumn Range", expected_count, count);
      ASSERT_EQ(expected_count, count);
    }
    {
      ScanSpec spec;
      auto pred = ColumnPredicate::IsNotNull(schema_.column(2));
      spec.AddPredicate(pred);
      ScanWithSpec(spec, &count);
      int expected_count = ExpectedCount(base_nrows_, base_cardinality_, base_null_upper_,
          base_cardinality_);
      ScopedTraceMsg("IsNotNull", expected_count, count);
      ASSERT_EQ(expected_count, count);
    }
    {
      ScanSpec spec;
      auto pred = ColumnPredicate::IsNull(schema_.column(2));
      spec.AddPredicate(pred);
      ScanWithSpec(spec, &count);
      int expected_count = ExpectedCount(base_nrows_, base_cardinality_, zero, base_null_upper_);
      ScopedTraceMsg("IsNull", expected_count, count);
      ASSERT_EQ(expected_count, count);
    }
  }

protected:
  RowOps rowops_;
  Schema schema_;
  int base_nrows_;
  int base_cardinality_;
  int base_null_upper_;
  bool has_been_altered_;
};

template<DataType KeyType, EncodingType Encoding>
struct KeyTypeWrapper {
  static const DataType type = KeyType;
  static const EncodingType encoding = Encoding;
};

typedef ::testing::Types<NumTypeRowOps<KeyTypeWrapper<INT8, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT8, PLAIN_ENCODING>>,
                         NumTypeRowOps<KeyTypeWrapper<INT8, RLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT16, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT16, PLAIN_ENCODING>>,
                         NumTypeRowOps<KeyTypeWrapper<INT16, RLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT32, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT32, PLAIN_ENCODING>>,
                         NumTypeRowOps<KeyTypeWrapper<INT32, RLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT64, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT64, PLAIN_ENCODING>>,
                         NumTypeRowOps<KeyTypeWrapper<FLOAT, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<FLOAT, PLAIN_ENCODING>>,
                         NumTypeRowOps<KeyTypeWrapper<DOUBLE, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<DOUBLE, PLAIN_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<STRING, DICT_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<STRING, PLAIN_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<STRING, PREFIX_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<BINARY, DICT_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<BINARY, PLAIN_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<BINARY, PREFIX_ENCODING>>
                         > KeyTypes;

TYPED_TEST_CASE(AllTypesScanCorrectnessTest, KeyTypes);

TYPED_TEST(AllTypesScanCorrectnessTest, AllNonNull) {
  this->FillTestTablet(10000, 21, 0);
  this->RunTests();
}

TYPED_TEST(AllTypesScanCorrectnessTest, SomeNull) {
  this->FillTestTablet(10000, 21, 5);
  this->RunTests();
}

TYPED_TEST(AllTypesScanCorrectnessTest, AllNull) {
  this->FillTestTablet(10000, 21, 21);
  this->RunTests();
}

}  // namespace tablet
}  // namespace kudu
