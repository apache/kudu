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
#ifndef KUDU_TABLET_COMPACTION_SVG_DUMP_H_
#define KUDU_TABLET_COMPACTION_SVG_DUMP_H_

#include <ostream>
#include <unordered_set>
#include <vector>

namespace kudu {
namespace tablet {

class RowSet;
class RowSetInfo;

// Dumps an SVG file which describes the rowset layout for the tablet replica
// and which highlights rowsets that would be selected for the next rowset
// compaction, if one were run. The SVG is printed to 'out', which must not be
// null. If 'print_xml_header' is true, prints an XML header including the xml
// tag and DOCTYPE. Otherwise, only the '<svg>...</svg>' section is printed.
void DumpCompactionSVG(const std::vector<RowSetInfo>& candidates,
                       const std::unordered_set<const RowSet*>& picked,
                       std::ostream* out,
                       bool print_xml_header);

// Like the above, but dumps the SVG to a file named according to the rules of
// --compaction_policy_dump_svgs_pattern. See the flag definition in svg_dump.cc.
void DumpCompactionSVGToFile(const std::vector<RowSetInfo>& candidates,
                             const std::unordered_set<const RowSet*>& picked);

} // namespace tablet
} // namespace kudu

#endif
