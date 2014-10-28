// Copyright 2014 Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_SERVER_WEBUI_UTIL_H
#define KUDU_SERVER_WEBUI_UTIL_H

#include <string>
#include <sstream>
#include <vector>

#include "kudu/gutil/ref_counted.h"

namespace kudu {

class Schema;
class MonitoredTask;

void HtmlOutputSchemaTable(const Schema& schema,
                           std::stringstream* output);
void HtmlOutputImpalaSchema(const std::string& table_name,
                            const Schema& schema,
                            const std::string& master_address,
                            std::stringstream* output);
void HtmlOutputTaskList(const std::vector<scoped_refptr<MonitoredTask> >& tasks,
                        std::stringstream* output);
} // namespace kudu

#endif // KUDU_SERVER_WEBUI_UTIL_H
