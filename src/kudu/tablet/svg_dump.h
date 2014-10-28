// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TABLET_COMPACTION_SVG_DUMP_H_
#define KUDU_TABLET_COMPACTION_SVG_DUMP_H_

#include <ostream>
#include <tr1/unordered_set>
#include <vector>

namespace kudu {
namespace tablet {

class RowSet;

class RowSetInfo;

// Dump an SVG file which represents the candidates
// for compaction, highlighting the ones that were selected.
// Dumps in to parameter ostream. If ostream is null, then default ostream
// specified as a flag is used (see svg_dump.cc).
// The last optional parameter controls whether to print an XML header in
// the file. If true, prints the header (xml tag and DOCTYPE). Otherwise, only
// the <svg>...</svg> section is printed.
void DumpCompactionSVG(const std::vector<RowSetInfo>& candidates,
                       const std::tr1::unordered_set<RowSet*>& picked,
                       std::ostream* out = NULL,
                       bool print_xml = true);

} // namespace tablet
} // namespace kudu

#endif
