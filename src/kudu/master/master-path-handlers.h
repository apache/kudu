// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_MASTER_MASTER_PATH_HANDLERS_H
#define KUDU_MASTER_MASTER_PATH_HANDLERS_H

#include "kudu/gutil/macros.h"
#include "kudu/server/webserver.h"

#include <string>
#include <sstream>
#include <vector>

namespace kudu {

class Schema;

namespace master {

class Master;
struct TabletReplica;
class TSDescriptor;
class TSRegistrationPB;

// Web page support for the master.
class MasterPathHandlers {
 public:
  explicit MasterPathHandlers(Master* master)
    : master_(master) {
  }

  ~MasterPathHandlers();

  Status Register(Webserver* server);

 private:
  void HandleTabletServers(const Webserver::WebRequest& req,
                           std::stringstream* output);
  void HandleCatalogManager(const Webserver::WebRequest& req,
                            std::stringstream* output);
  void HandleTablePage(const Webserver::WebRequest& req,
                       std::stringstream *output);
  void HandleMasters(const Webserver::WebRequest& req,
                     std::stringstream* output);
  void HandleDumpEntities(const Webserver::WebRequest& req,
                          std::stringstream* output);

  // Convert location of peers to HTML, indicating the roles
  // of each tablet server in a consensus configuration.
  // This method will display 'locations' in the order given.
  std::string RaftConfigToHtml(const std::vector<TabletReplica>& locations) const;

  // Convert the specified TSDescriptor to HTML, adding a link to the
  // tablet server's own webserver if specified in 'desc'.
  std::string TSDescriptorToHtml(const TSDescriptor& desc) const;

  // Convert the specified server registration to HTML, adding a link
  // to the server's own web server (if specified in 'reg') with
  // anchor text 'link_text'. 'RegistrationType' must be
  // TSRegistrationPB or MasterRegistrationPB.
  template<class RegistrationType>
  std::string RegistrationToHtml(const RegistrationType& reg,
                                 const std::string& link_text) const;

  Master* master_;
  DISALLOW_COPY_AND_ASSIGN(MasterPathHandlers);
};

void HandleTabletServersPage(const Webserver::WebRequest& req, std::stringstream* output);

} // namespace master
} // namespace kudu
#endif /* KUDU_MASTER_MASTER_PATH_HANDLERS_H */
