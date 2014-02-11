// Copyright 2014 Cloudera Inc.
#ifndef KUDU_SERVER_WEBUI_UTIL_H
#define KUDU_SERVER_WEBUI_UTIL_H

#include <string>
#include <sstream>

namespace kudu {

class Schema;

void HtmlOutputSchemaTable(const Schema& schema,
                           std::stringstream* output);
void HtmlOutputImpalaSchema(const std::string& table_name,
                            const Schema& schema,
                            std::stringstream* output);

} // namespace kudu

#endif // KUDU_SERVER_WEBUI_UTIL_H
