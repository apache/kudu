#!/usr/bin/python
# Copyright (c) 2013, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
# All rights reserved.

import MySQLdb as mdb
import sys
import os

if len(sys.argv) < 3:
  sys.exit("usage: %s <job_name> <days_count_to_fetch>" % sys.argv[0])

host = os.environ["MYSQLHOST"]
user = os.environ["MYSQLUSER"]
pwd = os.environ["MYSQLPWD"]
db = os.environ["MYSQLDB"]

con = mdb.connect(host, user, pwd, db)
with con:
  cur = con.cursor()
  job_name = sys.argv[1]
  days = sys.argv[2]
  cur.execute("select workload, runtime, build_number from kudu_perf_tpch where workload like %s AND curr_date >= DATE_SUB(NOW(), INTERVAL %s DAY) and runtime != 0 ORDER BY workload, build_number, curr_date", (job_name, days))
  rows = cur.fetchall()
  print 'workload', '\t', 'runtime', '\t', 'build_number'
  for row in rows:
    print row[0], '\t', row[1], '\t', row[2]

