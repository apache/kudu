#!/usr/bin/env python
# Copyright (c) 2014, Cloudera, inc.
#
# Simple HTTP server which receives test results from the build slaves and
# stores them in a MySQL database. The test logs are also stored in an S3 bucket.
#
# Configuration here is done via environment variables:
#
# MySQL config:
#   MYSQLHOST - host running mysql
#   MYSQLUSER - username
#   MYSQLPWD  - password
#   MYSQLDB   - mysql database
#
# S3 config:
#   AWS_ACCESS_KEY     - AWS access key
#   AWS_SECRET_KEY     - AWS secret key
#   TEST_RESULT_BUCKET - bucket to store results in (eg 'kudu-test-results')
#
# If these are not configured, the server is likely to crash.

import cherrypy
import boto
import logging
import MySQLdb as mdb
import os
import threading
import uuid

class TRServer(object):
  def __init__(self):
    self.thread_local = threading.local()
    self.ensure_table()
    self.s3 = self.connect_s3()
    self.s3_bucket = self.s3.get_bucket(os.environ["TEST_RESULT_BUCKET"])

  def connect_s3(self):
    access_key = os.environ["AWS_ACCESS_KEY"]
    secret_key = os.environ["AWS_SECRET_KEY"]
    s3 = boto.connect_s3(access_key, secret_key)
    logging.info("Connected to S3 with access key %s" % access_key)
    return s3


  def upload_to_s3(self, key, fp):
    k = boto.s3.key.Key(self.s3_bucket)
    k.key = key
    k.set_contents_from_string(fp.read(), reduced_redundancy=True)

  def connect_mysql(self):
    if hasattr(self.thread_local, "db"):
      return self.thread_local.db

    host = os.environ["MYSQLHOST"]
    user = os.environ["MYSQLUSER"]
    pwd = os.environ["MYSQLPWD"]
    db = os.environ["MYSQLDB"]
    self.thread_local.db = mdb.connect(host, user, pwd, db)
    logging.info("Connected to MySQL at %s" % host)
    return self.thread_local.db

  def ensure_table(self):
    c = self.connect_mysql().cursor()
    c.execute("""
      CREATE TABLE IF NOT EXISTS test_results (
        id int not null auto_increment primary key,
        timestamp timestamp not null default current_timestamp,
        build_id varchar(100),
        revision varchar(50),
        build_config varchar(100),
        hostname varchar(255),
        test_name varchar(100),
        status int,
        log_key char(40),
        INDEX (revision),
        INDEX (test_name),
        INDEX (timestamp)
      );""")

  @cherrypy.expose
  def index(self):
    return "Welcome to the test result server!"

  @cherrypy.expose
  def add_result(self, **kwargs):
    args = {}
    args.update(kwargs)

    # Only upload the log if it's provided.
    if 'log' in kwargs:
      log = kwargs['log']
      s3_id = uuid.uuid1()
      self.upload_to_s3(s3_id, log.file)
    else:
      s3_id = None
    args['log_key'] = s3_id

    logging.info("Handling report: %s" % repr(args))

    c = self.connect_mysql().cursor()
    c.execute("INSERT INTO test_results(build_id, revision, build_config, hostname, test_name, status, log_key) "
              "VALUES (%(build_id)s, %(revision)s, %(build_config)s, %(hostname)s, %(test_name)s,"
              "%(status)s, %(log_key)s)",
              args)
    return "Success!\n"

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  cherrypy.quickstart(TRServer())
