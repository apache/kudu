// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
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

namespace consensus {
class ConsensusStatePB;
} // namespace consensus

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
  void HandleScansPage(const Webserver::WebRequest& req,
                       std::stringstream* output);
  void HandleTabletsPage(const Webserver::WebRequest& req,
                         std::stringstream* output);
  void HandleTabletPage(const Webserver::WebRequest& req,
                        std::stringstream* output);
  void HandleTransactionsPage(const Webserver::WebRequest& req,
                              std::stringstream* output);
  void HandleTabletSVGPage(const Webserver::WebRequest& req,
                           std::stringstream* output);
  void HandleLogAnchorsPage(const Webserver::WebRequest& req,
                            std::stringstream* output);
  void HandleConsensusStatusPage(const Webserver::WebRequest& req,
                                 std::stringstream* output);
  void HandleDashboardsPage(const Webserver::WebRequest& req,
                            std::stringstream* output);
  void HandleMaintenanceManagerPage(const Webserver::WebRequest& req,
                                    std::stringstream* output);
  std::string ConsensusStatePBToHtml(const consensus::ConsensusStatePB& cstate) const;
  std::string ScannerToHtml(const Scanner& scanner) const;
  std::string IteratorStatsToHtml(const Schema& projection,
                                  const std::vector<IteratorStats>& stats) const;
  std::string GetDashboardLine(const std::string& link,
                               const std::string& text, const std::string& desc);

  TabletServer* tserver_;

  DISALLOW_COPY_AND_ASSIGN(TabletServerPathHandlers);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_TSERVER_PATH_HANDLERS_H */
