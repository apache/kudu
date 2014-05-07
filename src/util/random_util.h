// Copyright (c) 2014, Cloudera, inc.

#ifndef KUDU_UTIL_RANDOM_UTIL_H
#define KUDU_UTIL_RANDOM_UTIL_H

namespace kudu {

// Creates a normal distribution variable using the
// Box-Muller transform.
double NormalDist(double mean, double std_dev);

} // namespace kudu

#endif // KUDU_UTIL_RANDOM_UTIL_H
