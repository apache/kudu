// Copyright (c) 2014, Cloudera, inc.

#include "tablet/compaction_svg_dump.h"

#include <glog/logging.h>
#include <time.h>

#include <iostream>
#include <fstream>
#include <string>
#include <tr1/unordered_set>
#include <vector>

#include "gutil/strings/util.h"
#include "tablet/compaction_rowset_data.h"

using std::tr1::unordered_set;
using std::vector;

namespace kudu {
namespace tablet {
namespace compaction_policy {

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

namespace {

// Organize the input rowsets into rows for presentation.  This simply
// distributes 'rowsets' into separate vectors in 'rows' such that
// within any given row, none of the rowsets overlap in keyspace.
void OrganizeSVGRows(const vector<CompactionCandidate>& candidates,
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

} // anonymous namespace

void DumpCompactionSVG(const vector<CompactionCandidate>& candidates,
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
  out << "<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" "
         "\"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\">" << endl;
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
      out << StringPrintf("<rect x=\"%f\" y=\"%d\" width=\"%f\" height=\"%d\" "
                          "stroke=\"#000\" fill=\"%s\"/>",
                          x, y, width, kRowHeight, color) << endl;
      out << StringPrintf("<text x=\"%f\" y=\"%d\" width=\"%f\" height=\"%d\" "
                          "fill=\"rgb(0,0,0)\">%dMB</text>",
                          x, y + kRowHeight, width, kRowHeight, cand->size_mb()) << endl;
    }
  }

  out << "</svg>" << endl;
}

} // namespace compaction_policy
} // namespace tablet
} // namespace kudu
