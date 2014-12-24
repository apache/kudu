#!/usr/bin/python
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# Converts glog output from wal_hiccup into a CSV of all test runs.
#
# The output looks like this:
#
# var1,var2,...,varN,throughput,p95,p99,p99.99,max
# 0,0,...,68.5702,29,8544,22263,29108
#
# Each variable represents a different facet of the test setup.
# The throughput is in MB/s and the latency figures are in us.

import re
import sys

def main():
    if len(sys.argv) != 2:
        print "Usage: %s <log output>" % (sys.argv[0],)
        return
    cols = list()
    cols_printed = False
    with open(sys.argv[1], "r") as f:
        vals = None
        for line in f:
            line = line.strip()

            # Beginning of a test result.
            if "Test results for setup" in line:
                vals = list()

            # End of a test result.
            elif "-------" in line and vals is not None:
                if not cols_printed:
                    print ",".join(cols)
                    cols_printed = True
                print ",".join(vals)
                vals = None

            # Entry in a test result.
            elif vals is not None:
                m = re.match(".*\] ([\w\.]+): ([\d\.]+)", line)
                if not m:
                    continue
                col = m.group(1)
                val = m.group(2)
                if cols_printed:
                    if col not in cols:
                        raise Exception("Unexpected column %s" % (col,))
                else:
                    cols.append(col)
                vals.append(val)

if __name__ == '__main__':
    main()
