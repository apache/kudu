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
#include <vector>

#include "gutil/endian.h"
#include "gutil/map-util.h"
#include "gutil/strings/util.h"
#include "tablet/compaction.h"
#include "tablet/rowset.h"
#include "tablet/rowset_tree.h"
#include "util/knapsack_solver.h"
#include "util/slice.h"
#include "util/status.h"

using std::tr1::shared_ptr;

DEFINE_int32(budgeted_compaction_target_rowset_size, 32*1024*1024,
             "The target size for DiskRowSets during flush/compact when the "
             "budgeted compaction policy is used");

namespace kudu {
namespace tablet {

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
  : rowset_(rs) {
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
  const double kEpsilon = 0.0001;
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
      bool was_picked = ContainsKey(picked, cand->rowset().get());
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
    const shared_ptr<RowSet>& rs = cand.rowset();
    uint64_t this_size = rs->EstimateOnDiskSize();
    if (picked->size() < 2 || this_size < accumulated_size * 2) {
      InsertOrDie(picked, rs.get());
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

void BudgetedCompactionPolicy::CollectRowSetsBetween(const vector<CompactionCandidate> &rowsets,
                                                     double min, double max,
                                                     std::vector<CompactionCandidate> *results) {
  BOOST_FOREACH(const CompactionCandidate &candidate, rowsets) {
    if (candidate.cdf_min_key() >= min && candidate.cdf_max_key() <= max) {
      results->push_back(candidate);
    }
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

Status BudgetedCompactionPolicy::PickRowSets(const RowSetTree &tree,
                                             unordered_set<RowSet*>* picked) {
  vector<CompactionCandidate> all_candidates;
  SetupKnapsackInput(tree, &all_candidates);
  if (all_candidates.empty()) {
    // nothing to compact.
    return Status::OK();
  }

  KnapsackSolver<KnapsackTraits> solver;

  // The best set of rowsets chosen so far
  unordered_set<RowSet *> best_chosen;
  // The value attained by the 'best_chosen' solution.
  double best_optimal = 0;

  // Iterate over all pairs (a, b) of rowsets
  for (int i = 0; i < all_candidates.size(); i++) {
    const CompactionCandidate &cc_a = all_candidates[i];
    for (int j = i + 1; j < all_candidates.size(); j++) {
      const CompactionCandidate &cc_b = all_candidates[j];

      // Compute the "union width" -- i.e the width that the output would have
      // when compacting any set of rowsets that include both 'cc_a' and 'cc_b'.
      double ab_min = std::min(cc_a.cdf_min_key(), cc_b.cdf_min_key());
      double ab_max = std::max(cc_a.cdf_max_key(), cc_b.cdf_max_key());
      double union_width = ab_max - ab_min;
      DCHECK_GE(union_width, 0);
      DVLOG(2) << "Evaluating bounds:";
      DVLOG(2) << "cc_a: " << cc_a.ToString();
      DVLOG(2) << "cc_b: " << cc_b.ToString();
      DVLOG(2) << "Union width: " << union_width;

      // Only run knapsack against those candidates that fall entirely within
      // the union range.
      vector<CompactionCandidate> inrange_candidates;
      inrange_candidates.reserve(all_candidates.size());
      CollectRowSetsBetween(all_candidates, ab_min, ab_max, &inrange_candidates);

      vector<size_t> chosen;
      chosen.reserve(all_candidates.size());
      double this_optimal = 0;
      solver.Solve(inrange_candidates, size_budget_mb_, &chosen, &this_optimal);

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
      double adj_optimal = this_optimal - union_width * 1.01f;

      if (VLOG_IS_ON(2)) {
        VLOG(2) << "Solution=" << this_optimal << " adjusted=" << adj_optimal;
        BOOST_FOREACH(size_t i, chosen) {
          VLOG(2) << "  " << inrange_candidates[i].ToString();
        }
      }

      if (adj_optimal > best_optimal) {
        best_chosen.clear();
        BOOST_FOREACH(size_t i, chosen) {
          best_chosen.insert(inrange_candidates[i].rowset().get());
        }
        best_optimal = adj_optimal;
      }
    }
  }

  // Log the input and output of the selection.
  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Budgeted compaction selection:";
    BOOST_FOREACH(CompactionCandidate &cand, all_candidates) {
      const char *checkbox = "[ ]";
      if (ContainsKey(best_chosen, cand.rowset().get())) {
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
