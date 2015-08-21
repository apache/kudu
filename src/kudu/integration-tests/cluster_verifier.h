// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_INTEGRATION_TESTS_CLUSTER_VERIFIER_H
#define KUDU_INTEGRATION_TESTS_CLUSTER_VERIFIER_H

#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/tools/ksck.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

using tools::ChecksumOptions;

class ExternalMiniCluster;
class MonoDelta;

// Utility class for integration tests to verify that the cluster is in a good state.
class ClusterVerifier {
 public:
  explicit ClusterVerifier(ExternalMiniCluster* cluster);
  ~ClusterVerifier();

  // Set the amount of time which we'll retry trying to verify the cluster
  // state. We retry because it's possible that one of the replicas is behind
  // but in the process of catching up.
  void SetVerificationTimeout(const MonoDelta& timeout);

  /// Set the number of concurrent scans to execute per tablet server.
  void SetScanConcurrency(int concurrency);

  // Verify that the cluster is in good state. Triggers a gtest assertion failure
  // on failure.
  //
  // Currently, this just uses ksck to verify that the different replicas of each tablet
  // eventually agree.
  void CheckCluster();

  // Argument for CheckRowCount(...) below.
  enum ComparisonMode {
    AT_LEAST,
    EXACTLY
  };

  // Check that the given table has the given number of rows. Depending on ComparisonMode,
  // the comparison could be exact or a lower bound.
  //
  // Returns a Corruption Status if the row count is not as expected.
  //
  // NOTE: this does not perform any retries. If it's possible that the replicas are
  // still converging, it's best to use CheckCluster() first, which will wait for
  // convergence.
  void CheckRowCount(const std::string& table_name,
                     ComparisonMode mode,
                     int expected_row_count);

  // The same as above, but retries until a timeout elapses.
  void CheckRowCountWithRetries(const std::string& table_name,
                                ComparisonMode mode,
                                int expected_row_count,
                                const MonoDelta& timeout);

 private:
  Status DoKsck();

  // Implementation for CheckRowCount -- returns a Status instead of firing
  // gtest assertions.
  Status DoCheckRowCount(const std::string& table_name,
                         ComparisonMode mode,
                         int expected_row_count);


  ExternalMiniCluster* cluster_;

  ChecksumOptions checksum_options_;

  DISALLOW_COPY_AND_ASSIGN(ClusterVerifier);
};

} // namespace kudu
#endif /* KUDU_INTEGRATION_TESTS_CLUSTER_VERIFIER_H */
