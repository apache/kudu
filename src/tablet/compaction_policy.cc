// Copyright (c) 2013, Cloudera, inc.

#include "tablet/compaction_policy.h"
#include "tablet/compaction_policy-internal.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <time.h>

#include <algorithm>
#include <iostream>
#include <fstream>
#include <string>
#include <tr1/memory>
#include <utility>
#include <vector>

#include "gutil/endian.h"
#include "gutil/map-util.h"
#include "gutil/mathlimits.h"
#include "gutil/strings/util.h"
#include "tablet/compaction.h"
#include "tablet/rowset.h"
#include "tablet/rowset_tree.h"
#include "util/knapsack_solver.h"
#include "util/slice.h"
#include "util/status.h"

using std::tr1::shared_ptr;
using std::vector;

DEFINE_int32(budgeted_compaction_target_rowset_size, 32*1024*1024,
             "The target size for DiskRowSets during flush/compact when the "
             "budgeted compaction policy is used");

namespace kudu {
namespace tablet {

static const double kEpsilon = 0.0001;

// Adjust the result downward slightly for wider solutions.
// Consider this input:
//
//  |-----A----||----C----|
//  |-----B----|
//
// where A, B, and C are all 1MB, and the budget is 10MB.
//
// Without this tweak, the solution {A, B, C} has the exact same
// solution value as {A, B}, since both compactions would yield a
// tablet with average height 1. Since both solutions fit within
// the budget, either would be a valid pick, and it would be up
// to chance which solution would be selected.
// Intuitively, though, there's no benefit to including "C" in the
// compaction -- it just uses up some extra IO. If we slightly
// penalize wider solutions as a tie-breaker, then we'll pick {A, B}
// here.
static const double kSupportAdjust = 1.01;

using compaction_policy::CompactionCandidate;

// Flag to dump SVGs of every compaction decision.
//
// After dumping, these may be converted to an animation using a series of
// commands like:
// $ for x in compaction-*svg ; do convert $x $x.png ; done
// $ mencoder mf://compaction*png -mf fps=1 -ovc lavc -o compactions.avi

DEFINE_string(compaction_policy_dump_svgs_pattern, "",
              "File path into which to dump SVG visualization of "
              "selected compactions. This is mostly useful in "
              "the context of unit tests and benchmarks. "
              "The special string 'TIME' will be substituted "
              "with the compaction selection timestamp.");


////////////////////////////////////////////////////////////
// Implementations for internals
////////////////////////////////////////////////////////////
namespace compaction_policy {

DataSizeCDF::DataSizeCDF(const RowSetTree* tree)
  : tree_(tree) {
  total_size_ = ComputeTotalSize(tree->all_rowsets());
}

double DataSizeCDF::Evaluate(const Slice& key) const {
  if (total_size_ == 0) {
    return 1;
  }

  double contained_size = 0;

  // Find all rowsets which intersect the interval [-inf, key]
  vector<RowSet *> intersecting;
  tree_->FindRowSetsIntersectingInterval(Slice(), key, &intersecting);

  // For each of those, fractionally include it based on what percentage
  // of that rowset is contained within the interval. This assumes that
  // each rowset is itself uniformly distributed, which is an approximation,
  // but without collecting further metadata this is as good as we can do
  // without actually doing IO.
  BOOST_FOREACH(const RowSet *rs, intersecting) {
    Slice rs_min, rs_max;
    if (!rs->GetBounds(&rs_min, &rs_max).ok()) {
      VLOG(2) << "Ignoring " << rs->ToString() << " in CDF calculation";
      continue;
    }

    double fraction_contained = StringFractionInRange(rs_min, rs_max, key);
    contained_size += fraction_contained * rs->EstimateOnDiskSize();
  }
  DCHECK_LE(contained_size, total_size_);
  DCHECK_GE(contained_size, 0);
  double ret = contained_size / total_size_;
  CHECK_GE(ret, 0);
  return ret;
}

double DataSizeCDF::ComputeTotalSize(const RowSetVector& rowsets) {
  double ret = 0;
  BOOST_FOREACH(const shared_ptr<RowSet>& rs, rowsets) {
    ret += rs->EstimateOnDiskSize();
  }
  return ret;
}

double DataSizeCDF::StringFractionInRange(const Slice &min,
                                          const Slice &max,
                                          const Slice &point) {
  DCHECK_LE(min.compare(max), 0);
  if (point.compare(min) < 0) return 0;
  if (point.compare(max) >= 0) return 1;

  // Determine how much of a common prefix the strings share.
  int min_len = std::min(min.size(), max.size());
  int common_prefix = 0;
  while (common_prefix < min_len &&
         min[common_prefix] == max[common_prefix]) {
    common_prefix++;
  }

  DCHECK_EQ(memcmp(&min[0], &point[0], common_prefix), 0) << "point should share common prefix";

  // Convert the remaining portion of each string to an integer.
  uint64_t min_int = 0;
  memcpy(&min_int, &min[common_prefix],
         std::min(min.size() - common_prefix, sizeof(min_int)));
  min_int = BigEndian::ToHost64(min_int);

  uint64_t max_int = 0;
  memcpy(&max_int, &max[common_prefix],
         std::min(max.size() - common_prefix, sizeof(max_int)));
  max_int = BigEndian::ToHost64(max_int);

  uint64_t point_int = 0;
  memcpy(&point_int, &point[common_prefix],
         std::min(point.size() - common_prefix, sizeof(point_int)));
  point_int = BigEndian::ToHost64(point_int);

  // Compute how far between min and max the query point falls.
  return static_cast<double>(point_int - min_int) / (max_int - min_int);
}

////////////////////////////////////////////////////////////

void CompactionCandidate::CollectCandidates(const RowSetTree& tree,
                                            std::vector<CompactionCandidate>* candidates) {
  DataSizeCDF cdf(&tree);

  // Create CompactionCandidate objects for each RowSet, and calculate min/max
  // bounds for the whole tablet.
  BOOST_FOREACH(const shared_ptr<RowSet>& rs, tree.all_rowsets()) {
    if (rs->IsAvailableForCompaction()) {
      candidates->push_back(CompactionCandidate(cdf, rs));
    }
  }
}

CompactionCandidate::CompactionCandidate(const DataSizeCDF& cdf,
                                         const shared_ptr<RowSet>& rs)
  : rowset_(rs.get()) {
  size_mb_ = rs->EstimateOnDiskSize() / 1024 / 1024;

  Slice min, max;
  if (rs->GetBounds(&min, &max).ok()) {
    cdf_min_key_ = cdf.Evaluate(min);
    cdf_max_key_ = cdf.Evaluate(max);
    DCHECK_LE(cdf_min_key_, cdf_max_key_);
  } else {
    // The rowset doesn't know its own bounds. This counts as width
    // 1 because every access must check it.
    cdf_min_key_ = 0;
    cdf_max_key_ = 1;
  }
}

string CompactionCandidate::ToString() const {
  string ret;
  ret.append(rowset_->ToString());
  StringAppendF(&ret, "(% 3dM) [%.04f, %.04f]", size_mb_,
                cdf_min_key_, cdf_max_key_);
  Slice min, max;
  if (rowset_->GetBounds(&min, &max).ok()) {
    ret.append(" [").append(min.ToDebugString());
    ret.append(",").append(max.ToDebugString());
    ret.append("]");
  }
  return ret;
}

bool CompactionCandidate::Intersects(const CompactionCandidate &other) const {
  if (other.cdf_min_key() + kEpsilon > cdf_max_key()) return false;
  if (other.cdf_max_key() - kEpsilon < cdf_min_key()) return false;
  return true;
}

} // namespace compaction_policy

////////////////////////////////////////////////////////////
// Compaction visualization
////////////////////////////////////////////////////////////

// Organize the input rowsets into rows for presentation.  This simply
// distributes 'rowsets' into separate vectors in 'rows' such that
// within any given row, none of the rowsets overlap in keyspace.
static void OrganizeSVGRows(const vector<CompactionCandidate>& candidates,
                            vector<vector<const CompactionCandidate*> >* rows) {
  rows->push_back(vector<const CompactionCandidate *>());

  BOOST_FOREACH(const CompactionCandidate &candidate, candidates) {
    // Slot into the first row of the output which fits it
    bool found_slot = false;
    BOOST_FOREACH(vector<const CompactionCandidate *> &row, *rows) {
      // If this candidate doesn't intersect any other candidates in this
      // row, we can put it here.
      bool fits_in_row = true;
      BOOST_FOREACH(const CompactionCandidate *already_in_row, row) {
        if (candidate.Intersects(*already_in_row)) {
          fits_in_row = false;
          break;
        }
      }
      if (fits_in_row) {
        row.push_back(&candidate);
        found_slot = true;
        break;
      }
    }

    // If we couldn't find a spot in any existing row, add a new row
    // to the bottom of the SVG.
    if (!found_slot) {
      vector<const CompactionCandidate *> new_row;
      new_row.push_back(&candidate);
      rows->push_back(new_row);
    }
  }
}

// Dump an SVG file which represents the candidates
// for compaction, highlighting the ones that were selected.
static void DumpCompactionSVG(const vector<CompactionCandidate>& candidates,
                              const unordered_set<RowSet*>& picked) {
  const string &pattern = FLAGS_compaction_policy_dump_svgs_pattern;
  if (pattern.empty()) return;
  const string path = StringReplace(pattern, "TIME", StringPrintf("%ld", time(NULL)), true);

  using std::endl;
  std::ofstream out(path.c_str());
  if (!out.is_open()) {
    LOG(WARNING) << "Could not dump compaction output to " << path << ": file open failed";
    return;
  }

  vector<vector<const CompactionCandidate*> > svg_rows;
  OrganizeSVGRows(candidates, &svg_rows);

  const char *kPickedColor = "#f66";
  const char *kDefaultColor = "#666";
  const double kTotalWidth = 1200;
  const int kRowHeight = 15;
  const double kHeaderHeight = 60;
  const double kTotalHeight = kRowHeight * svg_rows.size() + kHeaderHeight;

  out << "<?xml version=\"1.0\" standalone=\"no\"?>" << endl;
  out << "<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\">" << endl;
  out << "<svg version=\"1.1\" width=\"" << kTotalWidth << "\" height=\"" << kTotalHeight << "\""
      << " viewBox=\"0 0 " << kTotalWidth << " " << kTotalHeight << "\""
      << " xmlns=\"http://www.w3.org/2000/svg\" >" << endl;

  // Background
  out << "<rect x=\"0.0\" y=\"0\" width=\"1200.0\" height=\"" << kTotalHeight << "\""
      << " fill=\"#fff\" />" << endl;

  // Title
  out << "<text text-anchor=\"middle\" x=\"" << (kTotalWidth / 2) << "\" "
      << "y=\"24\" font-size=\"17\" fill=\"#000\">Compaction</text>" << endl;

  for (int row_index = 0; row_index < svg_rows.size(); row_index++) {
    const vector<const CompactionCandidate *> &row = svg_rows[row_index];

    int y = kRowHeight * row_index + kHeaderHeight;
    BOOST_FOREACH(const CompactionCandidate *cand, row) {
      bool was_picked = ContainsKey(picked, cand->rowset());
      const char *color = was_picked ? kPickedColor : kDefaultColor;

      double x = cand->cdf_min_key() * kTotalWidth;
      double width = cand->width() * kTotalWidth;
      out << StringPrintf("<rect x=\"%f\" y=\"%d\" width=\"%f\" height=\"%d\" stroke=\"#000\" fill=\"%s\"/>",
                          x, y, width, kRowHeight, color) << endl;
      out << StringPrintf("<text x=\"%f\" y=\"%d\" width=\"%f\" height=\"%d\" fill=\"rgb(0,0,0)\">%dMB</text>",
                          x, y + kRowHeight, width, kRowHeight, cand->size_mb()) << endl;
    }
  }

  out << "</svg>" << endl;
}

////////////////////////////////////////////////////////////
// SizeRatioCompactionPolicy
////////////////////////////////////////////////////////////

static bool CompareBySize(const CompactionCandidate& a,
                          const CompactionCandidate& b) {
  return a.rowset()->EstimateOnDiskSize() < b.rowset()->EstimateOnDiskSize();
}

Status SizeRatioCompactionPolicy::PickRowSets(const RowSetTree &tree,
                                              std::tr1::unordered_set<RowSet*>* picked) {
  vector<CompactionCandidate> candidates;
  CompactionCandidate::CollectCandidates(tree, &candidates);

  // Sort the rowsets by their on-disk size
  std::sort(candidates.begin(), candidates.end(), CompareBySize);
  uint64_t accumulated_size = 0;
  BOOST_FOREACH(const CompactionCandidate &cand, candidates) {
    RowSet* rs = cand.rowset();
    uint64_t this_size = rs->EstimateOnDiskSize();
    if (picked->size() < 2 || this_size < accumulated_size * 2) {
      InsertOrDie(picked, rs);
      accumulated_size += this_size;
    } else {
      break;
    }
  }

  DumpCompactionSVG(candidates, *picked);

  return Status::OK();
}

////////////////////////////////////////////////////////////
// BudgetedCompactionPolicy
////////////////////////////////////////////////////////////

BudgetedCompactionPolicy::BudgetedCompactionPolicy(int budget)
  : size_budget_mb_(budget) {
  CHECK_GT(budget, 0);
}

uint64_t BudgetedCompactionPolicy::target_rowset_size() const {
  CHECK_GT(FLAGS_budgeted_compaction_target_rowset_size, 0);
  return FLAGS_budgeted_compaction_target_rowset_size;
}

struct CompareByDescendingDensity {
  bool operator()(const CompactionCandidate& a, const CompactionCandidate& b) const {
    return a.density() > b.density();
  }
};

static bool CompareByMinKey(const CompactionCandidate& a, const CompactionCandidate& b) {
  return a.cdf_min_key() < b.cdf_min_key();
}

static bool CompareByMaxKey(const CompactionCandidate& a, const CompactionCandidate& b) {
  return a.cdf_max_key() < b.cdf_max_key();
}


void BudgetedCompactionPolicy::SetupKnapsackInput(const RowSetTree &tree,
                                                  vector<CompactionCandidate> *rowsets) {
  CompactionCandidate::CollectCandidates(tree, rowsets);

  if (rowsets->size() < 2) {
    // require at least 2 rowsets to compact
    rowsets->clear();
    return;
  }

  // Enforce a minimum size of 1MB, since otherwise the knapsack algorithm
  // will always pick up small rowsets no matter what.
  BOOST_FOREACH(CompactionCandidate& candidate, *rowsets) {
    candidate.set_size_mb(std::max(candidate.size_mb(), 1));
  }
}

struct KnapsackTraits {
  typedef CompactionCandidate item_type;
  typedef double value_type;
  static int get_weight(const CompactionCandidate &item) {
    return item.size_mb();
  }
  static value_type get_value(const CompactionCandidate &item) {
    return item.width();
  }
};

// Incremental calculator for the upper bound on a knapsack solution,
// given a set of items. The upper bound is computed by solving the
// simpler "fractional knapsack problem" -- i.e the related problem
// in which each input may be fractionally put in the knapsack, instead
// of all-or-nothing. The fractional knapsack problem has a very efficient
// solution: sort by descending density and greedily choose elements
// until the budget is reached. The last element to be chosen may be
// partially included in the knapsack.
//
// Because this greedy solution only depends on sorting, it can be computed
// incrementally as items are considered by maintaining a min-heap, ordered
// by the density of the input elements. We need only maintain enough elements
// to satisfy the budget, making this logarithmic in the budget and linear
// in the number of elements added.
class UpperBoundCalculator {
 public:
  explicit UpperBoundCalculator(int max_weight)
    : total_weight_(0),
      total_value_(0),
      max_weight_(max_weight) {
  }

  void Add(const CompactionCandidate& candidate) {
    fractional_solution_.push_back(candidate);
    std::push_heap(fractional_solution_.begin(), fractional_solution_.end(),
                   CompareByDescendingDensity());

    total_weight_ += candidate.size_mb();
    total_value_ += candidate.width();
    const CompactionCandidate& top = fractional_solution_.front();
    if (total_weight_ - top.size_mb() >= max_weight_) {
      total_weight_ -= top.size_mb();
      total_value_ -= top.width();
      std::pop_heap(fractional_solution_.begin(), fractional_solution_.end(),
                    CompareByDescendingDensity());
      fractional_solution_.pop_back();
    }
  }

  // Compute the upper-bound to the 0-1 knapsack problem with the elements
  // added so far.
  double ComputeUpperBound() const {
    int excess_weight = total_weight_ - max_weight_;
    if (excess_weight <= 0) {
      return total_value_;
    }

    const CompactionCandidate& top = fractional_solution_.front();
    double fraction_of_top_to_remove = static_cast<double>(excess_weight) / top.size_mb();
    DCHECK_GT(fraction_of_top_to_remove, 0);
    return total_value_ - fraction_of_top_to_remove * top.width();
  }

  void clear() {
    fractional_solution_.clear();
    total_weight_ = 0;
    total_value_ = 0;
  }

 private:

  vector<CompactionCandidate> fractional_solution_;
  int total_weight_;
  double total_value_;
  int max_weight_;
};

Status BudgetedCompactionPolicy::PickRowSets(const RowSetTree &tree,
                                             unordered_set<RowSet*>* picked) {
  vector<CompactionCandidate> all_candidates;
  SetupKnapsackInput(tree, &all_candidates);
  if (all_candidates.empty()) {
    // nothing to compact.
    return Status::OK();
  }

  // Collect the rowsets ascending by min key and by max key.
  vector<CompactionCandidate> asc_min_key(all_candidates);
  std::sort(asc_min_key.begin(), asc_min_key.end(), CompareByMinKey);
  vector<CompactionCandidate> asc_max_key(all_candidates);
  std::sort(asc_max_key.begin(), asc_max_key.end(), CompareByMaxKey);

  UpperBoundCalculator ub_calc(size_budget_mb_);
  KnapsackSolver<KnapsackTraits> solver;

  // The best set of rowsets chosen so far
  unordered_set<RowSet *> best_chosen;
  // The value attained by the 'best_chosen' solution.
  double best_optimal = 0;

  vector<size_t> chosen_indexes;
  vector<CompactionCandidate> inrange_candidates;
  inrange_candidates.reserve(all_candidates.size());
  vector<double> upper_bounds;

  BOOST_FOREACH(const CompactionCandidate& cc_a, asc_min_key) {
    chosen_indexes.clear();
    inrange_candidates.clear();
    ub_calc.clear();
    upper_bounds.clear();

    double ab_min = cc_a.cdf_min_key();
    double ab_max = cc_a.cdf_max_key();

    // Collect all other candidates which would not expand the support to the
    // left of this one. Because these are sorted by ascending max key, we can
    // easily ensure that whenever we add a 'cc_b' to our candidate list for the
    // knapsack problem, we've already included all rowsets which fall in the
    // range from cc_a.min to cc_b.max.
    //
    // For example:
    //
    //  |-----A----|
    //      |-----B----|
    //         |----C----|
    //       |--------D-------|
    //
    // We process in the order: A, B, C, D.
    //
    // This saves us from having to iterate through the list again to find all
    // such rowsets.
    //
    // Additionally, each knapsack problem builds on the previous knapsack
    // problem by adding just a single rowset, meaning that we can reuse the
    // existing dynamic programming state to incrementally update the solution,
    // rather than having to rebuild from scratch.
    BOOST_FOREACH(const CompactionCandidate& cc_b, asc_max_key) {
      if (cc_b.cdf_min_key() < ab_min) {
        // Would expand support to the left.
        // TODO: possible optimization here: binary search to skip to the first
        // cc_b with cdf_max_key() > cc_a.cdf_min_key()
        continue;
      }
      inrange_candidates.push_back(cc_b);

      // While we're iterating, also calculate the upper bound for the solution
      // on the set within the [ab_min, ab_max] output range.
      ab_max = std::max(cc_b.cdf_max_key(), ab_max);
      double union_width = ab_max - ab_min;

      ub_calc.Add(cc_b);
      upper_bounds.push_back(ub_calc.ComputeUpperBound() - union_width * kSupportAdjust);
    }
    if (inrange_candidates.empty()) continue;
    // If the best upper bound across this whole range is worse than our current
    // optimal, we can short circuit all the knapsack-solving.
    if (*std::max_element(upper_bounds.begin(), upper_bounds.end()) < best_optimal) continue;

    solver.Reset(size_budget_mb_, &inrange_candidates);

    ab_max = cc_a.cdf_max_key();

    int i = 0;
    while (solver.ProcessNext()) {
      // If this candidate's upper bound is worse than the optimal, we don't
      // need to look at it.
      const CompactionCandidate& item = inrange_candidates[i];
      double upper_bound = upper_bounds[i];
      i++;
      if (upper_bound < best_optimal) continue;

      std::pair<int, double> best_with_this_item = solver.GetSolution();
      double best_value = best_with_this_item.second;

      ab_max = std::max(item.cdf_max_key(), ab_max);
      DCHECK_GE(ab_max, ab_min);
      double solution = best_value - (ab_max - ab_min) * kSupportAdjust;
      DCHECK_LT(solution, upper_bound + kEpsilon);

      if (solution > best_optimal) {
        solver.TracePath(best_with_this_item, &chosen_indexes);
        best_optimal = solution;
      }
    }

    // If we came up with a new solution, replace.
    if (!chosen_indexes.empty()) {
      best_chosen.clear();
      BOOST_FOREACH(int i, chosen_indexes) {
        best_chosen.insert(inrange_candidates[i].rowset());
      }
    }
  }

  // Log the input and output of the selection.
  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Budgeted compaction selection:";
    BOOST_FOREACH(CompactionCandidate &cand, all_candidates) {
      const char *checkbox = "[ ]";
      if (ContainsKey(best_chosen, cand.rowset())) {
        checkbox = "[x]";
      }
      VLOG(1) << "  " << checkbox << " " << cand.ToString();
    }
    VLOG(1) << "Solution value: " << best_optimal;
  }

  if (best_optimal <= 0) {
    LOG(INFO) << "Best compaction available makes things worse. Not compacting.";
    return Status::OK();
  }

  picked->swap(best_chosen);
  DumpCompactionSVG(all_candidates, *picked);

  return Status::OK();
}


} // namespace tablet
} // namespace kudu
