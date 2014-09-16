// Copyright (c) 2014 Cloudera, Inc.
#include "kudu/consensus/consensus_meta.h"

#include <gtest/gtest.h>

#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/server/metadata.pb.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

#define ASSERT_VALUES_EQUAL(cmeta, seqno, uuid, term) \
  ASSERT_NO_FATAL_FAILURE(AssertValuesEqual(cmeta, seqno, uuid, term))

namespace kudu {
namespace consensus {

using metadata::QuorumPB;
using std::string;

const char* kTabletId = "test-consensus-metadata";
const int64_t kInitialSeqno = 10;
const uint64_t kInitialTerm = 3;

class ConsensusMetadataTest : public KuduTest {
 public:
  ConsensusMetadataTest()
    : fs_manager_(env_.get(), test_dir_) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    ASSERT_OK(fs_manager_.CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_.Open());

    // Initialize test quorum.
    quorum_.set_seqno(kInitialSeqno);
    quorum_.add_peers()->set_permanent_uuid(fs_manager_.uuid());
  }

 protected:
  // Assert that the given cmeta has a single quorum with the given metadata values.
  void AssertValuesEqual(const ConsensusMetadata& cmeta,
                         int64_t seqno, const string& permanant_uuid, uint64_t term);

  FsManager fs_manager_;
  QuorumPB quorum_;
};

void ConsensusMetadataTest::AssertValuesEqual(const ConsensusMetadata& cmeta,
                                              int64_t seqno,
                                              const string& permanant_uuid,
                                              uint64_t term) {
  // Sanity checks.
  ASSERT_TRUE(cmeta.pb().committed_quorum().local());
  ASSERT_EQ(1, cmeta.pb().committed_quorum().peers_size());

  // Value checks.
  ASSERT_EQ(seqno, cmeta.pb().committed_quorum().seqno());
  ASSERT_EQ(permanant_uuid, cmeta.pb().committed_quorum().peers().begin()->permanent_uuid());
  ASSERT_EQ(term, cmeta.pb().current_term());
}

// Test the basic "happy case" of creating and then loading a file.
TEST_F(ConsensusMetadataTest, TestCreateLoad) {
  // Create the file.
  {
    gscoped_ptr<ConsensusMetadata> cmeta;
    ASSERT_OK(ConsensusMetadata::Create(&fs_manager_, kTabletId, quorum_, kInitialTerm, &cmeta));
  }

  // Load the file.
  gscoped_ptr<ConsensusMetadata> cmeta;
  ASSERT_OK(ConsensusMetadata::Load(&fs_manager_, kTabletId, &cmeta));
  ASSERT_VALUES_EQUAL(*cmeta, kInitialSeqno, fs_manager_.uuid(), kInitialTerm);
}

// Ensure that we get an error when loading a file that doesn't exist.
TEST_F(ConsensusMetadataTest, TestFailedLoad) {
  gscoped_ptr<ConsensusMetadata> cmeta;
  Status s = ConsensusMetadata::Load(&fs_manager_, kTabletId, &cmeta);
  ASSERT_TRUE(s.IsNotFound()) << "Unexpected status: " << s.ToString();
  LOG(INFO) << "Expected failure: " << s.ToString();
}

// Check that changes are not written to disk until Flush() is called.
TEST_F(ConsensusMetadataTest, TestFlush) {
  const uint64_t kNewTerm = 4;
  gscoped_ptr<ConsensusMetadata> cmeta;
  ASSERT_OK(ConsensusMetadata::Create(&fs_manager_, kTabletId, quorum_, kInitialTerm, &cmeta));
  cmeta->mutable_pb()->set_current_term(kNewTerm);

  // We are sort of "breaking the rules" by having multiple ConsensusMetadata
  // objects in flight that point to the same file, but for a test this is fine
  // since it's read-only.
  {
    gscoped_ptr<ConsensusMetadata> cmeta_read;
    ASSERT_OK(ConsensusMetadata::Load(&fs_manager_, kTabletId, &cmeta_read));
    ASSERT_VALUES_EQUAL(*cmeta_read, kInitialSeqno, fs_manager_.uuid(), kInitialTerm);
  }

  ASSERT_OK(cmeta->Flush());

  {
    gscoped_ptr<ConsensusMetadata> cmeta_read;
    ASSERT_OK(ConsensusMetadata::Load(&fs_manager_, kTabletId, &cmeta_read));
    ASSERT_VALUES_EQUAL(*cmeta_read, kInitialSeqno, fs_manager_.uuid(), kNewTerm);
  }
}

} // namespace consensus
} // namespace kudu
