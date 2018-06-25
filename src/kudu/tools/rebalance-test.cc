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

#include "kudu/tools/rebalancer.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/tools/rebalance_algo.h"
#include "kudu/util/test_macros.h"

using std::inserter;
using std::ostream;
using std::sort;
using std::string;
using std::transform;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

struct KsckReplicaSummaryInput {
  std::string ts_uuid;
  bool is_voter;
};

struct KsckServerHealthSummaryInput {
  std::string uuid;
};

struct KsckTabletSummaryInput {
  std::string id;
  std::string table_id;
  std::vector<KsckReplicaSummaryInput> replicas;
};

struct KsckTableSummaryInput {
  std::string id;
};

// The input to build KsckResults data. Contains relevant sub-fields of the
// KsckResults to use in the test.
struct KsckResultsInput {
  vector<KsckServerHealthSummaryInput> tserver_summaries;
  vector<KsckTabletSummaryInput> tablet_summaries;
  vector<KsckTableSummaryInput> table_summaries;
};

// The configuration for the test.
struct KsckResultsTestConfig {
  // The input for the test: ksck results.
  KsckResultsInput input;

  // The reference result of transformation of the 'input' field.
  ClusterBalanceInfo ref_balance_info;
};

KsckResults GenerateKsckResults(KsckResultsInput input) {
  KsckResults results;
  {
    vector<KsckServerHealthSummary>& summaries = results.tserver_summaries;
    for (const auto& summary_input : input.tserver_summaries) {
      KsckServerHealthSummary summary;
      summary.uuid = summary_input.uuid;
      summaries.emplace_back(std::move(summary));
    }
  }
  {
    vector<KsckTabletSummary>& summaries = results.tablet_summaries;
    for (const auto& summary_input : input.tablet_summaries) {
      KsckTabletSummary summary;
      summary.id = summary_input.id;
      summary.table_id = summary_input.table_id;
      auto& replicas = summary.replicas;
      for (const auto& replica_input : summary_input.replicas) {
        KsckReplicaSummary replica;
        replica.ts_uuid = replica_input.ts_uuid;
        replica.is_voter = replica_input.is_voter;
        replicas.emplace_back(std::move(replica));
      }
      summaries.emplace_back(std::move(summary));
    }
  }
  {
    vector<KsckTableSummary>& summaries = results.table_summaries;
    for (const auto& summary_input : input.table_summaries) {
      KsckTableSummary summary;
      summary.id = summary_input.id;
      summaries.emplace_back(summary);
    }
  }
  return results;
}

// The order of the key-value pairs whose keys compare equivalent is the order
// of insertion and does not change. Since the insertion order is not
// important for the comparison with the reference results, this comparison
// operator normalizes both the 'lhs' and 'rhs', so the comparison operator
// compares only the contents of the 'servers_by_replica_count', not the order
// of the elements.
bool HasSameContents(const ServersByCountMap& lhs,
                     const ServersByCountMap& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  auto it_lhs = lhs.begin();
  auto it_rhs = rhs.begin();
  for (; it_lhs != lhs.end() && it_rhs != rhs.end(); ) {
    auto key_lhs = it_lhs->first;
    auto key_rhs = it_rhs->first;
    if (key_lhs != key_rhs) {
      return false;
    }

    auto eq_range_lhs = lhs.equal_range(key_lhs);
    auto eq_range_rhs = rhs.equal_range(key_rhs);

    vector<string> lhs_values;
    {
      transform(eq_range_lhs.first, eq_range_lhs.second,
                inserter(lhs_values, lhs_values.begin()),
                [](const ServersByCountMap::value_type& elem) {
                  return elem.second;
                });
      sort(lhs_values.begin(), lhs_values.end());
    }

    vector<string> rhs_values;
    {
      transform(eq_range_rhs.first, eq_range_rhs.second,
                inserter(rhs_values, rhs_values.begin()),
                [](const ServersByCountMap::value_type& elem) {
                  return elem.second;
                });
      sort(rhs_values.begin(), rhs_values.end());
    }

    if (lhs_values != rhs_values) {
      return false;
    }

    // Advance the iterators to continue with next key.
    it_lhs = eq_range_lhs.second;
    it_rhs = eq_range_rhs.second;
  }

  return true;
}

} // anonymous namespace

bool operator==(const TableBalanceInfo& lhs, const TableBalanceInfo& rhs) {
  return HasSameContents(lhs.servers_by_replica_count,
                         rhs.servers_by_replica_count);
}

bool operator==(const ClusterBalanceInfo& lhs, const ClusterBalanceInfo& rhs) {
  return
      lhs.table_info_by_skew == rhs.table_info_by_skew &&
      HasSameContents(lhs.servers_by_total_replica_count,
                      rhs.servers_by_total_replica_count);
}

ostream& operator<<(ostream& s, const ClusterBalanceInfo& info) {
  s << "[";
  for (const auto& elem : info.servers_by_total_replica_count) {
    s << " " << elem.first << ":" << elem.second;
  }
  s << " ]; [";
  for (const auto& elem : info.table_info_by_skew) {
    s << " " << elem.first << ":{ " << elem.second.table_id
      << " [";
    for (const auto& e : elem.second.servers_by_replica_count) {
      s << " " << e.first << ":" << e.second;
    }
    s << " ] }";
  }
  s << " ]";
  return s;
}

// Test converting KsckResults result into ClusterBalanceInfo.
TEST(KuduKsckRebalanceTest, KsckResultsToClusterBalanceInfo) {
  const KsckResultsTestConfig kConfigs[] = {
    // Empty
    {
      {},
      {}
    },
    // One tserver, one table, one tablet, RF=1.
    {
      {
        { { "ts_0" }, },
        { { "tablet_0", "table_a", { { "ts_0", true }, }, }, },
        { { "table_a" }, },
      },
      {
        { { 0, { "table_a", { { 1, "ts_0" }, } } }, },
        { { 1, "ts_0" }, },
      }
    },
    // Balanced configuration: three tservers, one table, one tablet, RF=3.
    {
      {
        { { "ts_0" }, { "ts_1" }, { "ts_2" }, },
        {
          { "tablet_a0", "table_a", { { "ts_0", true }, }, },
          { "tablet_a0", "table_a", { { "ts_1", true }, }, },
          { "tablet_a0", "table_a", { { "ts_2", true }, }, },
        },
        { { "table_a", } },
      },
      {
        {
          { 0, { "table_a", {
                { 1, "ts_2" },
                { 1, "ts_1" },
                { 1, "ts_0" },
              }
            }
          },
        },
        {
          { 1, "ts_2" }, { 1, "ts_1" }, { 1, "ts_0" },
        },
      }
    },
    // Simple unbalanced configuration.
    {
      {
        { { "ts_0" }, { "ts_1" }, { "ts_2" }, },
        {
          { "tablet_a_0", "table_a", { { "ts_0", true }, }, },
          { "tablet_b_0", "table_b", { { "ts_0", true }, }, },
          { "tablet_c_0", "table_c", { { "ts_0", true }, }, },
        },
        { { { "table_a" }, { "table_b" }, { "table_c" }, } },
      },
      {
        {
          { 1, { "table_c", {
                { 0, "ts_1" }, { 0, "ts_2" }, { 1, "ts_0" },
              }
            }
          },
          { 1, { "table_b", {
                { 0, "ts_1" }, { 0, "ts_2" }, { 1, "ts_0" },
              }
            }
          },
          { 1, { "table_a", {
                { 0, "ts_1" }, { 0, "ts_2" }, { 1, "ts_0" },
              }
            }
          },
        },
        {
          { 0, "ts_2" }, { 0, "ts_1" }, { 3, "ts_0" },
        },
      }
    },
    // table_a: 1 tablet with RF=3
    // table_b: 3 tablets with RF=1
    // table_c: 2 tablets with RF=1
    {
      {
        { { "ts_0" }, { "ts_1" }, { "ts_2" }, },
        {
          { "tablet_a_0", "table_a", { { "ts_0", true }, }, },
          { "tablet_a_0", "table_a", { { "ts_1", true }, }, },
          { "tablet_a_0", "table_a", { { "ts_2", true }, }, },
          { "tablet_b_0", "table_b", { { "ts_0", true }, }, },
          { "tablet_b_1", "table_b", { { "ts_0", true }, }, },
          { "tablet_b_2", "table_b", { { "ts_0", true }, }, },
          { "tablet_c_0", "table_c", { { "ts_1", true }, }, },
          { "tablet_c_1", "table_c", { { "ts_1", true }, }, },
        },
        { { { "table_a" }, { "table_b" }, { "table_c" }, } },
      },
      {
        {
          { 2, { "table_c", {
                { 0, "ts_0" }, { 0, "ts_2" }, { 2, "ts_1" },
              }
            }
          },
          { 3, { "table_b", {
                { 0, "ts_1" }, { 0, "ts_2" }, { 3, "ts_0" },
              }
            }
          },
          { 0, { "table_a", {
                { 1, "ts_2" }, { 1, "ts_1" }, { 1, "ts_0" },
              }
            }
          },
        },
        {
          { 1, "ts_2" }, { 3, "ts_1" }, { 4, "ts_0" },
        },
      }
    },
  };

  for (auto idx = 0; idx < arraysize(kConfigs); ++idx) {
    SCOPED_TRACE(Substitute("test config index: $0", idx)); \
    const auto& cfg = kConfigs[idx];
    auto ksck_results = GenerateKsckResults(cfg.input);
    ClusterBalanceInfo cbi;
    Rebalancer rebalancer({});
    ASSERT_OK(rebalancer.KsckResultsToClusterBalanceInfo(
        ksck_results, Rebalancer::MovesInProgress(), &cbi));
    ASSERT_EQ(cfg.ref_balance_info, cbi);
  }
}

} // namespace tools
} // namespace kudu
