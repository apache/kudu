// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_UTIL_ENV_UTIL_H
#define KUDU_UTIL_ENV_UTIL_H

#include <tr1/memory>
#include <string>

#include "util/env.h"

namespace kudu {
namespace env_util {

using std::string;
using std::tr1::shared_ptr;

Status OpenFileForWrite(Env *env, const string &path,
                        shared_ptr<WritableFile> *file);

}
}

#endif
