// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_INTEGRATION_TESTS_CLUSTER_VERIFIER_H
#define KUDU_INTEGRATION_TESTS_CLUSTER_VERIFIER_H

#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

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

  // Verify that the cluster is in good state. Triggers a gtest assertion failure
  // on failure.
  //
  // Currently, this just uses ksck to verify that the different replicas of each tablet
  // eventually agree.
  void CheckCluster();

  // Check that the given table has exactly the given number of rows.
  // Triggers a gtest assertion failure if the table has either fewer or a greater
  // number of rows.
  //
  // NOTE: this does not perform any retries. If it's possible that the replicas are
  // still converging, it's best to use CheckCluster() first, which will wait for
  // convergence.
  void CheckRowCount(const std::string& table_name,
                     int expected_row_count);

 private:
  Status DoKsck();

  ExternalMiniCluster* cluster_;

  // The maximum amount of time to loop trying to verify the cluster integrity.
  MonoDelta verification_timeout_;

  DISALLOW_COPY_AND_ASSIGN(ClusterVerifier);
};

} // namespace kudu
#endif /* KUDU_INTEGRATION_TESTS_CLUSTER_VERIFIER_H */
