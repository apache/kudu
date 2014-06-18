// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_TABLET_COMPACTION_SVG_DUMP_H_
#define KUDU_TABLET_COMPACTION_SVG_DUMP_H_

#include <tr1/unordered_set>
#include <vector>

namespace kudu {
namespace tablet {

class RowSet;

namespace compaction_policy {

class CompactionCandidate;
class DataSizeCDF;

// Dump an SVG file which represents the candidates
// for compaction, highlighting the ones that were selected.
void DumpCompactionSVG(const std::vector<CompactionCandidate>& candidates,
                       const std::tr1::unordered_set<RowSet*>& picked);

} // namespace compaction_policy
} // namespace tablet
} // namespace kudu

#endif
