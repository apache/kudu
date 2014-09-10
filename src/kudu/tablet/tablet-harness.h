// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_PEER_HARNESS_H
#define KUDU_TABLET_TABLET_PEER_HARNESS_H

#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/consensus/opid_anchor_registry.h"
#include "kudu/server/logical_clock.h"
#include "kudu/server/metadata.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

using std::string;
using std::vector;

namespace kudu {
namespace tablet {

class TabletHarness {
 public:
  struct Options {
    explicit Options(const string& root_dir)
      : env(Env::Default()),
        tablet_id("test_tablet_id"),
        root_dir(root_dir),
        enable_metrics(true) {
    }

    Env* env;
    string tablet_id;
    string root_dir;
    bool enable_metrics;
  };

  TabletHarness(const Schema& schema,
                const Options& options)
    : options_(options),
      schema_(schema) {
  }

  Status Open() {
    metadata::TabletMasterBlockPB master_block;
    master_block.set_table_id("KuduTableTestId");
    master_block.set_tablet_id(options_.tablet_id);
    master_block.set_block_a("00000000000000000000000000000000");
    master_block.set_block_b("11111111111111111111111111111111");

    // Build a schema with IDs
    Schema server_schema = SchemaBuilder(schema_).Build();

    quorum_.set_seqno(0);

    // Build the Tablet
    fs_manager_.reset(new FsManager(options_.env, options_.root_dir));
    RETURN_NOT_OK(fs_manager_->CreateInitialFileSystemLayout());
    scoped_refptr<metadata::TabletMetadata> metadata;
    RETURN_NOT_OK(metadata::TabletMetadata::LoadOrCreate(fs_manager_.get(),
                                                         master_block,
                                                         "KuduTableTest",
                                                         server_schema,
                                                         quorum_,
                                                         "", "",
                                                         metadata::REMOTE_BOOTSTRAP_DONE,
                                                         &metadata));
    RETURN_NOT_OK_PREPEND(metadata::TabletMetadata::PersistMasterBlock(
                            fs_manager_.get(), master_block),
                          "Couldn't persist test tablet master block");
    if (options_.enable_metrics) {
      metrics_registry_.reset(new MetricRegistry());
      metrics_.reset(new MetricContext(metrics_registry_.get(), "tablet-harness"));
    }

    clock_ = server::LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp);
    tablet_.reset(new Tablet(metadata,
                             clock_,
                             metrics_.get(),
                             new log::OpIdAnchorRegistry()));
    RETURN_NOT_OK(tablet_->Open());
    return Status::OK();
  }

  server::Clock* clock() const {
    return clock_.get();
  }

  const std::tr1::shared_ptr<Tablet>& tablet() {
    return tablet_;
  }

  FsManager* fs_manager() {
    return fs_manager_.get();
  }

 private:
  Options options_;

  gscoped_ptr<MetricRegistry> metrics_registry_;
  gscoped_ptr<MetricContext> metrics_;

  scoped_refptr<server::Clock> clock_;
  Schema schema_;
  metadata::QuorumPB quorum_;
  std::tr1::shared_ptr<Tablet> tablet_;
  gscoped_ptr<FsManager> fs_manager_;
};

} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_TABLET_PEER_HARNESS_H */
