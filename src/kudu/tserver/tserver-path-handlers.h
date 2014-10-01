// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TSERVER_PATH_HANDLERS_H
#define KUDU_TSERVER_TSERVER_PATH_HANDLERS_H

#include "kudu/gutil/macros.h"
#include "kudu/server/webserver.h"
#include <string>
#include <sstream>
#include <vector>

namespace kudu {

class Schema;
struct IteratorStats;

namespace metadata {
class QuorumPB;
}

namespace tserver {

class TabletServer;
class Scanner;

class TabletServerPathHandlers {
 public:
  explicit TabletServerPathHandlers(TabletServer* tserver)
    : tserver_(tserver) {
  }

  ~TabletServerPathHandlers();

  Status Register(Webserver* server);

 private:
  void HandleScansPage(const Webserver::ArgumentMap& args,
                       std::stringstream* output);
  void HandleTabletsPage(const Webserver::ArgumentMap& args,
                         std::stringstream* output);
  void HandleTabletPage(const Webserver::ArgumentMap& args,
                        std::stringstream* output);
  void HandleTransactionsPage(const Webserver::ArgumentMap& args,
                              std::stringstream* output);
  void HandleTabletSVGPage(const Webserver::ArgumentMap& args,
                           std::stringstream* output);
  void HandleConsensusStatusPage(const Webserver::ArgumentMap& args,
                                 std::stringstream* output);
  void HandleDashboardsPage(const Webserver::ArgumentMap& args,
                            std::stringstream* output);
  std::string QuorumPBToHtml(const metadata::QuorumPB& quorum) const;
  std::string ScannerToHtml(const Scanner& scanner) const;
  std::string IteratorStatsToHtml(const std::vector<IteratorStats>& stats) const;

  TabletServer* tserver_;

  DISALLOW_COPY_AND_ASSIGN(TabletServerPathHandlers);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_TSERVER_PATH_HANDLERS_H */
