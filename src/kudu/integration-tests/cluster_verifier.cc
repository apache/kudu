// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/util/monotime.h"
#include "kudu/util/test_util.h"

using std::string;

namespace kudu {

using tools::Ksck;
using tools::KsckCluster;
using tools::KsckMaster;
using tools::RemoteKsckMaster;

ClusterVerifier::ClusterVerifier(ExternalMiniCluster* cluster)
  : cluster_(cluster),
    checksum_options_(ChecksumOptions()) {
}

ClusterVerifier::~ClusterVerifier() {
}

void ClusterVerifier::SetVerificationTimeout(const MonoDelta& timeout) {
  checksum_options_.timeout = timeout;
}

void ClusterVerifier::SetScanConcurrency(int concurrency) {
  checksum_options_.scan_concurrency = concurrency;
}

void ClusterVerifier::CheckCluster() {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(checksum_options_.timeout);

  Status s;
  while (MonoTime::Now(MonoTime::FINE).ComesBefore(deadline)) {
    s = DoKsck();
    if (s.ok()) {
      break;
    }

    LOG(INFO) << "Check not successful yet, sleeping and retrying: " + s.ToString();
    SleepFor(MonoDelta::FromSeconds(1));
  }
  ASSERT_OK(s);
}

Status ClusterVerifier::DoKsck() {
  string addr = cluster_->leader_master()->bound_rpc_addr().ToString();

  shared_ptr<KsckMaster> master(new RemoteKsckMaster(addr));
  shared_ptr<KsckCluster> cluster(new KsckCluster(master));
  shared_ptr<Ksck> ksck(new Ksck(cluster));

  // This is required for everything below.
  RETURN_NOT_OK(ksck->CheckMasterRunning());
  RETURN_NOT_OK(ksck->FetchTableAndTabletInfo());
  RETURN_NOT_OK(ksck->CheckTabletServersRunning());
  RETURN_NOT_OK(ksck->CheckTablesConsistency());

  vector<string> tables;
  vector<string> tablets;
  RETURN_NOT_OK(ksck->ChecksumData(tables, tablets, checksum_options_));
  return Status::OK();
}

void ClusterVerifier::CheckRowCount(const std::string& table_name,
                                    int expected_row_count) {
  shared_ptr<client::KuduClient> client;
  client::KuduClientBuilder builder;
  ASSERT_OK(cluster_->CreateClient(builder,
                                   &client));
  scoped_refptr<client::KuduTable> table;
  ASSERT_OK(client->OpenTable(table_name, &table));
  client::KuduScanner scanner(table.get());
  client::KuduSchema empty_projection(vector<client::KuduColumnSchema>(), 0);
  ASSERT_OK(scanner.SetProjection(&empty_projection));
  ASSERT_OK(scanner.Open());
  int count = 0;
  vector<client::KuduRowResult> rows;
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&rows));
    count += rows.size();
    rows.clear();
  }
  ASSERT_EQ(count, expected_row_count);
}

} // namespace kudu
