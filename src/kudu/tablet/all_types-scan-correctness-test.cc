#include <glog/logging.h>
#include <gtest/gtest.h>

#include <kudu/util/flags.h>

#include "kudu/common/schema.h"
#include "kudu/tablet/tablet-test-base.h"

namespace kudu {
namespace tablet {

// Column numbers; kColX refers to column with name "val_x".
static const int kColA = 1;
static const int kColB = 2;
static const int kColC = 3;

// Rows added after altering and before altering the table.
static const int kNumAddedRows = 2000;
static const int kNumBaseRows = 10000;

// Number of unique values in each column.
static const int kCardinality = 21;

// Lower and upper bounds of the predicates.
static const int kLower = 3;
static const int kUpper = 10;

// Number of elements to allocate memory for. Each range predicate requires two elements, and each
// call to AddColumn(read_default) requires one. Must be greater than the number of elements
// actually needed.
static const int kNumAllocatedElements = 32;

// Strings and binaries will have the following string length.
static const int kStrlen = 10;

struct RowOpsBase {
  RowOpsBase(DataType type, EncodingType encoding) : type_(type), encoding_(encoding) {
    schema_ = Schema({ColumnSchema("key", INT32),
                     ColumnSchema("val_a", type, true, nullptr, nullptr,
                         ColumnStorageAttributes(encoding, DEFAULT_COMPRESSION)),
                     ColumnSchema("val_b", type, true, nullptr, nullptr,
                         ColumnStorageAttributes(encoding, DEFAULT_COMPRESSION))}, 1);

  }
  DataType type_;
  EncodingType encoding_;
  Schema schema_;
  Schema altered_schema_;
};

template<typename KeyTypeWrapper>
struct SliceTypeRowOps : public RowOpsBase {
  SliceTypeRowOps() : RowOpsBase(KeyTypeWrapper::type, KeyTypeWrapper::encoding),
    strs_(kNumAllocatedElements), slices_(kNumAllocatedElements), cur(0) {}

  // Assumes the string representation of n is under strlen characters.
  std::string LeftZeroPadded(int n, int strlen) {
    return StringPrintf(Substitute("%0$0$1", strlen, PRId64).c_str(), static_cast<int64_t>(n));
  }

  void GenerateRow(int value, bool altered, KuduPartialRow* row) {
    if (value < 0) {
      CHECK_OK(row->SetNull(kColA));
      CHECK_OK(row->SetNull(kColB));
      if (altered) {
        CHECK_OK(row->SetNull(kColC));
      }
    } else {
      std::string string_a = LeftZeroPadded(value, 10);
      std::string string_b = LeftZeroPadded(value, 10);
      std::string string_c = LeftZeroPadded(value, 10);
      Slice slice_a(string_a);
      Slice slice_b(string_b);
      Slice slice_c(string_c);
      CHECK_OK(row->SetSliceCopy<TypeTraits<KeyTypeWrapper::type>>(kColA, slice_a));
      CHECK_OK(row->SetSliceCopy<TypeTraits<KeyTypeWrapper::type>>(kColB, slice_b));
      if (altered) {
        CHECK_OK(row->SetSliceCopy<TypeTraits<KeyTypeWrapper::type>>(kColC, slice_c));
      }
    }
  }

  void* GenerateElement(int value) {
    strs_[cur] = LeftZeroPadded(value, kStrlen);
    slices_[cur] = Slice(strs_[cur]);
    return &slices_[cur++];
  }

  ColumnPredicate GenerateRangePredicate(const Schema& schema, size_t col, int lower, int upper) {
    // Key predicate strings and slices in scope in a vector.
    strs_[cur] = LeftZeroPadded(lower, 10);
    strs_[cur + 1] = LeftZeroPadded(upper, 10);
    slices_[cur] = Slice(strs_[cur]);
    slices_[cur + 1] = Slice(strs_[cur + 1]);
    auto pred = ColumnPredicate::Range(schema.column(col), &slices_[cur], &slices_[cur + 1]);
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
    nums_(kNumAllocatedElements), cur(0) {}

  typedef typename TypeTraits<KeyTypeWrapper::type>::cpp_type CppType;

  void GenerateRow(int value, bool altered, KuduPartialRow* row) {
    if (value < 0) {
      CHECK_OK(row->SetNull(kColA));
      CHECK_OK(row->SetNull(kColB));
      if (altered) {
        CHECK_OK(row->SetNull(kColC));
      }
    } else {
      row->Set<TypeTraits<KeyTypeWrapper::type>>(kColA, value);
      row->Set<TypeTraits<KeyTypeWrapper::type>>(kColB, value);
      if (altered) {
        row->Set<TypeTraits<KeyTypeWrapper::type>>(kColC, value);
      }
    }
  }

  void* GenerateElement(int value) {
    nums_[cur] = value;
    return &nums_[cur++];
  }

  ColumnPredicate GenerateRangePredicate(const Schema& schema, size_t col, int lower, int upper) {
    nums_[cur] = lower;
    nums_[cur + 1] = upper;
    auto pred = ColumnPredicate::Range(schema.column(col), &nums_[cur], &nums_[cur + 1]);
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
int ExpectedCount(int nrows, int cardinality, int lower_val, int upper_val) {
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

std::string TraceMsg(const std::string& test_name,  int expected, int actual) {
  std::ostringstream ss;
  ss << test_name << " Scan Results: " << expected << " expected, " << actual << " returned.";
  return ss.str();
}

// Tests for correctness by running predicates on repeated patterns of rows, specified by nrows,
// cardinality, and null_upper. RowOps is a specialized RowOpsBase, defining how the tablet should
// add rows and generate predicates for various types.
template <class RowOps>
class AllTypesScanCorrectnessTest : public KuduTabletTest {
public:
  AllTypesScanCorrectnessTest() : KuduTabletTest(RowOps().schema_), rowops_(RowOps()),
      schema_(rowops_.schema_), altered_schema_(schema_),
      base_nrows_(0), base_cardinality_(0), base_null_upper_(0), added_nrows_(0) {}

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
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);
    for (int i = 0; i < nrows; i++) {
      CHECK_OK(row.SetInt32(0, i));

      // Populate the bottom of the repeating pattern with NULLs.
      // Note: Non-positive null_upper will yield a completely non-NULL column.
      if (i % cardinality < null_upper) {
        rowops_.GenerateRow(-1, false, &row);
      } else {
        rowops_.GenerateRow(i % cardinality, false, &row);
      }
      ASSERT_OK_FAST(writer.Insert(row));
    }
    ASSERT_OK(tablet()->Flush());
  }

  // Adds the above pattern to the table with keys starting after the base rows.
  void FillAlteredTestTablet(int nrows) {
    added_nrows_ = nrows;
    LocalTabletWriter writer(tablet().get(), &altered_schema_);
    KuduPartialRow row(&altered_schema_);
    for (int i = 0; i < nrows; i++) {
      CHECK_OK(row.SetInt32(0, base_nrows_ + i));
      if (i % base_cardinality_ < base_null_upper_) {
        rowops_.GenerateRow(-1, true, &row);
      } else {
        rowops_.GenerateRow(i % base_cardinality_, true, &row);
      }
      ASSERT_OK_FAST(writer.Upsert(row));
    }
    ASSERT_OK(tablet()->Flush());
  }

  // Adds a column called "val_c" to the schema with the specified read-default.
  void AddColumn(int read_default) {
    void* default_ptr;
    if (read_default < 0) {
      default_ptr = nullptr;
    } else {
      default_ptr = rowops_.GenerateElement(read_default);
    }
    SchemaBuilder builder(tablet()->metadata()->schema());
    builder.RemoveColumn("val_c");
    ASSERT_OK(builder.AddColumn("val_c", rowops_.type_, true, default_ptr, nullptr));
    AlterSchema(builder.Build());
    altered_schema_ = Schema({ColumnSchema("key", INT32),
                     ColumnSchema("val_a", rowops_.type_, true, nullptr, nullptr,
                         ColumnStorageAttributes(rowops_.encoding_, DEFAULT_COMPRESSION)),
                     ColumnSchema("val_b", rowops_.type_, true, nullptr, nullptr,
                         ColumnStorageAttributes(rowops_.encoding_, DEFAULT_COMPRESSION)),
                     ColumnSchema("val_c", rowops_.type_, true, default_ptr, nullptr,
                         ColumnStorageAttributes(rowops_.encoding_, DEFAULT_COMPRESSION))}, 1);
  }

  // Scan the results of a query. Set "count" to the number of results satisfying the predicates.
  // ScanSpec must have all desired predicates already added to it.
  void ScanWithSpec(const Schema& schema, ScanSpec spec, int* count) {
    Arena arena(1028, 1028);
    AutoReleasePool pool;
    *count = 0;
    spec.OptimizeScan(schema, &arena, &pool, true);
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(schema, &iter));
    ASSERT_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicate.";
    ASSERT_OK(SilentIterateToStringList(iter.get(), count));
  }

  void RunTests() {
    RunUnalteredTabletTests();
    RunAlteredTabletTests();
  }

  // Runs queries on an un-altered table. Correctness is determined by comparing the number of rows
  // returned with the number of rows expected by each query.
  void RunUnalteredTabletTests() {
    int lower_non_null = kLower;
    if (kLower < base_null_upper_) {
      lower_non_null = base_null_upper_;
    }

    int count = 0;
    {
      ScanSpec spec;
      auto pred = rowops_.GenerateRangePredicate(schema_, kColA, kLower, kUpper);
      spec.AddPredicate(pred);
      ScanWithSpec(schema_, spec, &count);
      int expected_count = ExpectedCount(base_nrows_, base_cardinality_, lower_non_null, kUpper);
      SCOPED_TRACE(TraceMsg("Range", expected_count, count));
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
      ColumnPredicate pred_a = rowops_.GenerateRangePredicate(schema_, kColA, 0, kUpper);
      spec.AddPredicate(pred_a);
      ColumnPredicate pred_b = rowops_.GenerateRangePredicate(schema_, kColB, kLower,
                                                              base_cardinality_);
      spec.AddPredicate(pred_b);
      ScanWithSpec(schema_, spec, &count);
      int expected_count = ExpectedCount(base_nrows_, base_cardinality_, lower_non_null, kUpper);
      SCOPED_TRACE(TraceMsg("MultiColumn Range", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      ScanSpec spec;
      auto pred = ColumnPredicate::IsNotNull(schema_.column(kColB));
      spec.AddPredicate(pred);
      ScanWithSpec(schema_, spec, &count);
      int expected_count = ExpectedCount(base_nrows_, base_cardinality_, base_null_upper_,
          base_cardinality_);
      SCOPED_TRACE(TraceMsg("IsNotNull", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
  }

  // Runs tests with an altered table. Queries are run with different read-defaults and are deemed
  // correct if they return the correct number of results.
  void RunAlteredTabletTests() {
    int lower_non_null = kLower;
    // Determine the lowest non-null value in the data range. Used in getting expected counts.
    if (kLower < base_null_upper_) {
      lower_non_null = base_null_upper_;
    }
    // Tests with null read-default. Ex. case: (-: null, 1: pred satisfied, 0: pred not satisfied)
    // kLower: 5, kUpper: 8
    // Base nrows: 30                 |Altered nrows: 25
    // Cardinality: 20, null_upper: 3, read_default: NULL
    // Predicate: (val_b >= 0 && val_b < 8) && (val_c >= 5 && val_c < 20)
    //  0    5    10   15   20   25   |30   35   40   45   50     key
    // [---11111000000000000---1111100|---11111000000000000---11] val_b
    // [------------------------------|---00111111111111111---00] val_c
    // [000000000000000000000000000000|0000011100000000000000000] Result
    AddColumn(-1);
    FillAlteredTestTablet(kNumAddedRows);
    int count = 0;
    {
      ScanSpec spec;
      // val_b >= kLower && val_b <= kUpper && val_c is not null
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, kLower, kUpper);
      auto pred_c = ColumnPredicate::IsNotNull(altered_schema_.column(kColC));
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      // Since the table has a null read-default on the added column, the IsNotNull predicate
      // should filter out all rows in the base data.
      int base_expected_count = 0;
      int altered_expected_count = ExpectedCount(added_nrows_,
                                                 base_cardinality_,
                                                 lower_non_null,
                                                 kUpper);
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Null default, Range+IsNotNull", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      ScanSpec spec;
      // val_b >= 0 && val_b < kUpper && val_c >= kLower && val_c < cardinality
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, 0, kUpper);
      auto pred_c = rowops_.GenerateRangePredicate(altered_schema_, kColC, kLower,
                                                   base_cardinality_);
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      // Since the added column has a null read-default, the base rows will be completely filtered.
      int base_expected_count = 0;
      // The added data will be predicated with [lower, upper).
      int altered_expected_count = ExpectedCount(added_nrows_,
                                                 base_cardinality_,
                                                 lower_non_null,
                                                 kUpper);
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Null default, Range", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    // Tests with non-null read-default. Ex. case:
    // kLower: 5, kUpper: 8
    // Base nrows: 30                |Altered nrows: 25
    // Cardinality: 20, null_upper: 3, read_default: 7
    // Predicate: (val_b >= 0 && val_b < 8) && (val_c >= 5 && val_c < 20)
    //  0    5    10   15   20   25   |30   35   40   45   50     key
    // [---11111000000000000---1111100|---11111000000000000---11] val_b
    // [111111111111111111111111111111|---00111111111111111---00] val_c
    // [000111110000000000000001111100|0000011100000000000000000] Result
    int read_default = 7;
    AddColumn(read_default);
    FillAlteredTestTablet(kNumAddedRows);
    {
      ScanSpec spec;
      // val_b >= kLower && val_b <= kUpper && val_c is not null
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, kLower, kUpper);
      auto pred_c = ColumnPredicate::IsNotNull(altered_schema_.column(kColC));
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      int base_expected_count = ExpectedCount(base_nrows_, base_cardinality_, lower_non_null,
                                              kUpper);
      // Since the new column has the same data as the base columns, IsNotNull with a Range
      // predicate should yield the same rows as the Range query alone on the altered data.
      int altered_expected_count = ExpectedCount(added_nrows_, base_cardinality_, lower_non_null,
                                                 kUpper);
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Non-null default, Range+IsNotNull", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      int lower = 0;
      ScanSpec spec;
      // val_b >= 0 && val_b < kUpper && val_c >= kLower && val_c < cardinality
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, 0, kUpper);
      auto pred_c = rowops_.GenerateRangePredicate(altered_schema_, kColC, kLower,
                                                   base_cardinality_);
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      // Take into account possible null values when calculating expected counts.
      if (base_null_upper_ > 0) {
        lower = base_null_upper_;
      }
      // Because the read_default is in range, the predicate on "val_c" will be satisfied
      // by base data, and all rows that satisfy "pred_b" will be returned.
      int base_expected_count = ExpectedCount(base_nrows_, base_cardinality_, lower, kUpper);
      int altered_expected_count = ExpectedCount(added_nrows_,
                                                 base_cardinality_,
                                                 lower_non_null,
                                                 kUpper);
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Non-null default, Range with Default", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      // Used to ensure the query does not select the read-default.
      int default_plus_one = read_default + 1;
      ScanSpec spec;
      // val_b >= 0 && val_b < kUpper && val_c >= read_default + 1 && val_c < cardinality
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, 0, kUpper);
      auto pred_c = rowops_.GenerateRangePredicate(altered_schema_, kColC, default_plus_one,
                                                   base_cardinality_);
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      if (default_plus_one < base_null_upper_) {
        default_plus_one = base_null_upper_;
      }
      // Because the read_default is out of range, the "pred_c" will not be satisfied by base data,
      // so all base rows will be filtered.
      int base_expected_count = 0;
      int altered_expected_count = ExpectedCount(added_nrows_,
                                                 base_cardinality_,
                                                 default_plus_one,
                                                 kUpper);
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Non-null default, Range without Default", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
  }

protected:
  RowOps rowops_;
  Schema schema_;
  Schema altered_schema_;
  int base_nrows_;
  int base_cardinality_;
  int base_null_upper_;
  int added_nrows_;
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
  int null_upper = 0;
  this->FillTestTablet(kNumBaseRows, kCardinality, null_upper);
  this->RunTests();
}

TYPED_TEST(AllTypesScanCorrectnessTest, SomeNull) {
  int null_upper = kUpper/2;
  this->FillTestTablet(kNumBaseRows, kCardinality, null_upper);
  this->RunTests();
}

TYPED_TEST(AllTypesScanCorrectnessTest, AllNull) {
  int null_upper = kCardinality;
  this->FillTestTablet(kNumBaseRows, kCardinality, null_upper);
  this->RunTests();
}

}  // namespace tablet
}  // namespace kudu
