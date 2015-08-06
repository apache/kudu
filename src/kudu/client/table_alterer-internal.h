// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_TABLE_ALTERER_INTERNAL_H
#define KUDU_CLIENT_TABLE_ALTERER_INTERNAL_H

#include <boost/optional.hpp>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/master/master.pb.h"
#include "kudu/util/status.h"

namespace kudu {
namespace master {
class AlterTableRequestPB_AlterColumn;
} // namespace master
namespace client {

class KuduColumnSpec;

class KuduTableAlterer::Data {
 public:
  Data(KuduClient* client, const std::string& name);
  ~Data();
  Status ToRequest(master::AlterTableRequestPB* req);


  KuduClient* const client_;
  const std::string table_name_;

  Status status_;

  struct Step {
    master::AlterTableRequestPB::StepType step_type;

    // Owned by KuduTableAlterer::Data.
    KuduColumnSpec *spec;
  };
  std::vector<Step> steps_;

  MonoDelta timeout_;

  bool wait_;

  boost::optional<std::string> rename_to_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
