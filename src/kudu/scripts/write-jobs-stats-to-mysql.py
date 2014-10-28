#!/usr/bin/python
# Copyright (c) 2013, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
# All rights reserved.

import MySQLdb as mdb
import sys
import os

if len(sys.argv) < 6:
  sys.exit("usage: %s <job_name> <build_number> <workload> <iteration> <runtime>" % sys.argv[0])

host = os.environ["MYSQLHOST"]
user = os.environ["MYSQLUSER"]
pwd = os.environ["MYSQLPWD"]
db = os.environ["MYSQLDB"]

con = mdb.connect(host, user, pwd, db)
print "Connected to mysql"
with con:
  cur = con.cursor()
  job_name = sys.argv[1]
  build_number = sys.argv[2]
  workload = sys.argv[3]
  iteration = sys.argv[4]
  runtime = sys.argv[5]
  cur.execute("INSERT INTO kudu_perf_tpch VALUES(%s, %s, %s, %s, %s, DEFAULT)",
              (job_name, build_number, workload, iteration, runtime))
  rows = cur.fetchall()

