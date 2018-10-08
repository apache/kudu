// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tablet/svg_dump.h"

#include <algorithm>
#include <ctime>
#include <string>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/tablet/rowset_info.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

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
TAG_FLAG(compaction_policy_dump_svgs_pattern, hidden);

using std::endl;
using std::ostream;
using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

namespace {

// Organize the input rowsets into rows for presentation.  This simply
// distributes 'rowsets' into separate vectors in 'rows' such that
// within any given row, none of the rowsets overlap in keyspace.
void OrganizeSVGRows(const vector<RowSetInfo>& candidates,
                     vector<vector<const RowSetInfo*>>* rows) {
  DCHECK(rows);
  rows->clear();
  for (const auto& candidate : candidates) {
    // Slot into the first row which fits it.
    bool found_slot = false;
    for (auto& row : *rows) {
      // If this candidate doesn't intersect any other rowsets already in this
      // row, we can put it in this row.
      auto fits_in_row = std::none_of(row.begin(),
                                      row.end(),
                                      [&candidate](const RowSetInfo* already_in_row) {
                                        return candidate.Intersects(*already_in_row);
                                      });
      if (fits_in_row) {
        row.push_back(&candidate);
        found_slot = true;
        break;
      }
    }
    // If we couldn't find a spot in any existing row, add a new row
    // to the bottom of the SVG.
    if (!found_slot) {
      rows->push_back({ &candidate });
    }
  }
}

void DumpSVG(const vector<RowSetInfo>& candidates,
             const unordered_set<RowSet*>& picked,
             ostream* outptr) {
  CHECK(outptr);
  CHECK(outptr->good());
  ostream& out = *outptr;

  vector<vector<const RowSetInfo*>> svg_rows;
  OrganizeSVGRows(candidates, &svg_rows);

  const char *kPickedColor = "#f66"; // Light red.
  const char *kDefaultColor = "#666"; // Dark gray.
  constexpr double kTotalWidth = 1200.0;
  constexpr int kRowHeight = 15;
  constexpr double kHeaderHeight = 60.0;
  const double kTotalHeight = kRowHeight * svg_rows.size() + kHeaderHeight;

  out << Substitute(
      R"(<svg version="1.1" width="$0" height="$1" viewBox="0 0 $0 $1" )"
      R"(xmlns="http://www.w3.org/2000/svg">)",
      kTotalWidth, kTotalHeight)
      << endl;

  // Background.
  out << Substitute(R"(<rect x="0" y="0" width="$0" height="$1" fill="#fff"/>)",
                    kTotalWidth, kTotalHeight)
      << endl;

  for (auto row_index = 0; row_index < svg_rows.size(); row_index++) {
    const vector<const RowSetInfo *> &row = svg_rows[row_index];

    const auto y = kRowHeight * row_index + kHeaderHeight;
    for (const RowSetInfo *cand : row) {
      const char *color = ContainsKey(picked, cand->rowset()) ? kPickedColor :
                                                                kDefaultColor;
      const auto x = cand->cdf_min_key() * kTotalWidth;
      const auto width = cand->width() * kTotalWidth;
      out << Substitute(
          R"(<rect x="$0" y="$1" width="$2" height="$3" stroke="#000" fill="$4"/>)",
          x, y, width, kRowHeight, color)
          << endl;
      out << Substitute(R"+(<text x="$0" y="$1" width="$2" height="$3" )+"
                        R"+(fill="rgb(0,0,0)">$4MB</text>)+",
                        x, y + kRowHeight, width, kRowHeight, cand->size_mb())
          << endl;
    }
  }

  out << "</svg>" << endl;
}

void PrintXMLHeader(ostream* out) {
  CHECK(out);
  CHECK(out->good());
  *out << R"(<?xml version="1.0" standalone="no"?>)"
       << endl
       << R"(<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" )"
       << R"("http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">)"
       << endl;
}

} // anonymous namespace

void DumpCompactionSVG(const vector<RowSetInfo>& candidates,
                       const unordered_set<RowSet*>& picked,
                       ostream* out,
                       bool print_xml_header) {
  CHECK(out);
  VLOG(1) << "Dumping SVG of DiskRowSetLayout with"
          << (print_xml_header ? "" : "out") << " XML header";
  if (print_xml_header) {
    PrintXMLHeader(out);
  }
  DumpSVG(candidates, picked, out);
}

void DumpCompactionSVGToFile(const vector<RowSetInfo>& candidates,
                             const unordered_set<RowSet*>& picked) {
  const string& pattern = FLAGS_compaction_policy_dump_svgs_pattern;
  if (pattern.empty()) {
    return;
  }
  const string path = StringReplace(pattern,
                                    "TIME",
                                    StringPrintf("%ld", time(nullptr)),
                                    /*replace_all=*/true);
  std::ostringstream buf;
  DumpCompactionSVG(candidates, picked, &buf, /*print_xml_header=*/true);
  WARN_NOT_OK(WriteStringToFile(Env::Default(), buf.str(), path),
              "unable to dump rowset compaction SVG to file");
}

} // namespace tablet
} // namespace kudu
