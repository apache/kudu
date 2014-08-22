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
#
# Installation instructions:
#   You probably want to run this inside a virtualenv to avoid having
#   to install python modules systemwide. For example:
#     $ virtualenv ~/flaky-test-server-env/
#     $ . ~/flaky-test-server-env/bin/activate
#     $ pip install boto
#     $ pip install jinja2
#     $ pip install cherrypy
#     $ pip install MySQL-python

import boto
import cherrypy
from jinja2 import Template
import logging
import MySQLdb
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


  def upload_to_s3(self, key, fp, filename):
    k = boto.s3.key.Key(self.s3_bucket)
    k.key = key
    # The Content-Disposition header sets the filename that the browser
    # will use to download this.
    # We have to cast to str() here, because boto will try to escape the header
    # incorrectly if you pass a unicode string.
    k.set_metadata('Content-Disposition', str('inline; filename=%s' % filename))
    k.set_contents_from_string(fp.read(),
                               reduced_redundancy=True)

  def connect_mysql(self):
    if hasattr(self.thread_local, "db") and \
          self.thread_local.db is not None:
      return self.thread_local.db

    host = os.environ["MYSQLHOST"]
    user = os.environ["MYSQLUSER"]
    pwd = os.environ["MYSQLPWD"]
    db = os.environ["MYSQLDB"]
    self.thread_local.db = MySQLdb.connect(host, user, pwd, db)
    logging.info("Connected to MySQL at %s" % host)
    return self.thread_local.db

  def execute_query(self, query, *args):
    """ Execute a query, automatically reconnecting on disconnection. """
    # We'll try up to 3 times to reconnect
    MAX_ATTEMPTS = 3

    # Error code for the "MySQL server has gone away" error.
    MYSQL_SERVER_GONE_AWAY = 2006

    attempt_num = 0
    while True:
      c = self.connect_mysql().cursor(MySQLdb.cursors.DictCursor)
      attempt_num = attempt_num + 1
      try:
        c.execute(query, *args)
        return c
      except MySQLdb.OperationalError as err:
        if err.args[0] == MYSQL_SERVER_GONE_AWAY and attempt_num < MAX_ATTEMPTS:
          logging.warn("Forcing reconnect to MySQL: %s" % err)
          self.thread_local.db = None
          continue
        else:
          raise


  def ensure_table(self):
    c = self.execute_query("""
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
      self.upload_to_s3(s3_id, log.file, log.filename)
    else:
      s3_id = None
    args['log_key'] = s3_id

    logging.info("Handling report: %s" % repr(args))

    self.execute_query(
      "INSERT INTO test_results(build_id, revision, build_config, hostname, test_name, status, log_key) "
      "VALUES (%(build_id)s, %(revision)s, %(build_config)s, %(hostname)s, %(test_name)s,"
      "%(status)s, %(log_key)s)",
      args)
    return "Success!\n"

  @cherrypy.expose
  def download_log(self, key):
    expiry = 60 * 60 * 24 # link should last 1 day
    k = boto.s3.key.Key(self.s3_bucket)
    k.key = key
    raise cherrypy.HTTPRedirect(k.generate_url(expiry))

  def recently_failed_html(self):
    """ Return an HTML report of recently failed tests """
    c = self.execute_query(
      "SELECT * from test_results WHERE status != 0 "
      "AND timestamp > NOW() - INTERVAL 1 WEEK "
      "ORDER BY timestamp DESC")
    failed_tests = c.fetchall()
    template = Template("""
    <h1>Failed in last week</h1>
    <table class="table">
      <tr>
        <th>time</th>
        <th>build</th>
        <th>rev</th>
        <th>machine</th>
        <th>test</th>
        <th>config</th>
        <th>exit code</th>
      </tr>
      {% for run in failed_tests %}
        <tr>
          <td>{{ run.timestamp |e }}</td>
          <td>{{ run.build_id |e }}</td>
          <td>{{ run.revision |e }}</td>
          <td>{{ run.hostname |e }}</td>
          <td><a href="/test_drilldown?test_name={{ run.test_name |urlencode }}">
              {{ run.test_name |e }}
              </a></td>
          <td>{{ run.build_config |e }}</td>
          <td>{{ run.status |e }}
            {% if run.log_key %}
              <a href="/download_log?key={{ run.log_key |urlencode }}">failure log</a>
            {% endif %}
          </td>
        </tr>
      {% endfor %}
    </table>
    """)
    return template.render(failed_tests=failed_tests)

  def flaky_report_html(self):
    """ Return an HTML report of recently flaky tests """
    c = self.execute_query(
              """SELECT
                   test_name,
                   revision,
                   MIN(timestamp) AS first_run,
                   SUM(IF(status != 0, 1, 0)) AS num_failures,
                   COUNT(*) AS num_runs
                 FROM test_results
                 WHERE timestamp > NOW() - INTERVAL 1 WEEK
                 GROUP BY test_name, revision
                 HAVING num_failures > 0
                 ORDER BY test_name, first_run DESC""")
    r = c.fetchall()
    return Template("""
    <h1>Flaky in last week</h1>
    <table class="table">
      <tr>
       <th>test</th>
       <th>rev</th>
       <th>failure rate</th>
      </tr>
      {% for r in results %}
      <tr>
        <td><a href="/test_drilldown?test_name={{ r.test_name |urlencode }}">
              {{ r.test_name |e }}
            </a></td>
        <td>{{ r.revision |e }}</td>
        <td>{{ r.num_failures |e }} / {{ r.num_runs }}</td>
      </tr>
      {% endfor %}
    </table>
    """).render(results=r)

  @cherrypy.expose
  def list_failed_tests(self, build_pattern, num_days):
    num_days = int(num_days)
    c = self.execute_query(
              """SELECT DISTINCT
                   test_name
                 FROM test_results
                 WHERE timestamp > NOW() - INTERVAL %(num_days)s DAY
                   AND status != 0
                   AND build_id LIKE %(build_pattern)s""",
              dict(build_pattern=build_pattern,
                   num_days=num_days))
    cherrypy.response.headers['Content-Type'] = 'text/plain'
    return "\n".join(row['test_name'] for row in c.fetchall())

  @cherrypy.expose
  def test_drilldown(self, test_name):

    # Get summary statistics for the test, grouped by revision
    c = self.execute_query(
              """SELECT
                   revision,
                   MIN(timestamp) AS first_run,
                   SUM(IF(status != 0, 1, 0)) AS num_failures,
                   COUNT(*) AS num_runs
                 FROM test_results
                 WHERE timestamp > NOW() - INTERVAL 1 MONTH
                   AND test_name = %(test_name)s
                 GROUP BY revision
                 ORDER BY first_run DESC""",
              dict(test_name=test_name))
    revision_rows = c.fetchall()

    # Convert to a dictionary, by revision
    rev_dict = dict( [(r['revision'], r) for r in revision_rows] )

    # Add an empty 'runs' array to each revision to be filled in below
    for r in revision_rows:
      r['runs'] = []

    # Append the specific info on failures
    c.execute("SELECT * from test_results "
              "WHERE timestamp > NOW() - INTERVAL 1 WEEK "
              "AND test_name = %(test_name)s",
              dict(test_name=test_name))
    for failure in c.fetchall():
      rev_dict[failure['revision']]['runs'].append(failure)

    return self.render_container(Template("""
    <h1>{{ test_name |e }} flakiness over recent revisions</h1>
    {% for r in revision_rows %}
      <h4>{{ r.revision }} (Failed {{ r.num_failures }} / {{ r.num_runs }})</h4>
      <a data-toggle="collapse" href="#rev-{{r.revision|e}}">details</a>
      <div class="collapse" id="rev-{{r.revision}}">
        <table class="table">
          <tr>
            <th>time</th>
            <th>build</th>
            <th>rev</th>
            <th>machine</th>
            <th>test</th>
            <th>config</th>
            <th>exit code</th>
          </tr>
          {% for run in r.runs %}
            <tr {% if run.status != 0 %}
                  style="background-color: #faa;"
                {% else %}
                  style="background-color: #afa;"
                {% endif %}>
              <td>{{ run.timestamp |e }}</td>
              <td>{{ run.build_id |e }}</td>
              <td>{{ run.revision |e }}</td>
              <td>{{ run.hostname |e }}</td>
              <td>{{ run.test_name |e }}</td>
              <td>{{ run.build_config |e }}</td>
              <td>{{ run.status |e }}
                {% if run.log_key %}
                  <a href="/download_log?key={{ run.log_key |e }}">failure log</a>
                {% endif %}
              </td>
            </tr>
          {% endfor %}
        </table>
      </div>
    {% endfor %}
    """).render(revision_rows=revision_rows, test_name=test_name))

  @cherrypy.expose
  def index(self):
    body = self.recently_failed_html()
    body += "<hr/>"
    body += self.flaky_report_html()
    return self.render_container(body)

  def render_container(self, body):
    """ Render the "body" HTML inside of a bootstrap container page. """
    template = Template("""
    <!DOCTYPE html>
    <html>
      <head><title>Kudu test results</title>
      <link rel="stylesheet" href="//maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css" />
    </head>
    <body>
      <script src="//ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js"></script>
      <script src="//maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>
      <div class="container-fluid">
      {{ body }}
      </div>
    </body>
    </html>
    """)
    return template.render(body=body)

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  cherrypy.config.update(
    {'server.socket_host': '0.0.0.0'} )
  cherrypy.quickstart(TRServer())
