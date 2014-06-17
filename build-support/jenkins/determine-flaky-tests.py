#!/usr/bin/env python
# Copyright (c) 2014, Cloudera, inc.
#
# Script to look at recent Jenkins history and find any unit tests which have
# recently failed. Outputs a list of these tests on stdout.

from collections import defaultdict
import logging
from optparse import OptionParser
import os
import re
import shutil
import simplejson
import time
import urllib2

# Configuration
DEFAULT_JENKINS_URL = "http://sandbox.jenkins.cloudera.com"
DEFAULT_JOB_NAME = "kudu-test"
NUM_PREVIOUS_DAYS = 3

# Constants
SECONDS_PER_DAY = 86400
FAILED_TEST_RE = re.compile('Test.*: (\S+).*\*\*\*Failed')

class TestFailure(object):
  """ Simple struct-like class to record a test failure """

  def __init__(self, test_name, build_number, url):
    self.test_name = test_name
    self.build_number = build_number
    self.url = url


def parse_args():
  parser = OptionParser()
  parser.add_option("-J", "--jenkins-url", type="string",
                    help="Jenkins URL", default=DEFAULT_JENKINS_URL)
  parser.add_option("-j", "--job-name", type="string", action="append",
                    dest="job_names", help="Job name to look at (may specify multiple times)",
                    default=[DEFAULT_JOB_NAME])
  parser.add_option("-D", "--download", type="string",
                    help="directory to download failed tests to")
  parser.add_option("-d", "--days", type=int, default=3,
                    help="number of days of test history to analyze")
  (options, args) = parser.parse_args()
  if args:
    parser.error("unexpected arguments: " + repr(args))
  return options

""" List all builds of the target project. """
def list_builds(jenkins_url, job_name):
  url = "%(jenkins)s/job/%(job_name)s/api/json?tree=builds[number,result,timestamp]" % dict(
    jenkins=jenkins_url,
    job_name=job_name)
  try:
    data = simplejson.load(urllib2.urlopen(url))
  except:
    logging.error("Could not fetch: %s" % url)
    raise
  return data['builds']

""" List any sub-builds of the given build number which failed """
def get_failing_subbuilds(jenkins_url, job_name, build_number):
  url = "%(jenkins)s/job/%(job_name)s/%(build_number)d/api/json?tree=runs[url,result]" % dict(
    jenkins=jenkins_url,
    job_name=job_name,
    build_number=build_number)
  data = simplejson.load(urllib2.urlopen(url))
  return [d['url'] for d in data['runs'] if d['result'] == 'FAILURE']

""" Find the names of any tests which failed in the given build output URL. """
def find_failing_tests(url):
  ret = set()
  page = urllib2.urlopen(url)
  for line in page.readlines():
    m = FAILED_TEST_RE.search(line)
    if m:
      ret.add(m.group(1))
  return ret


def find_flaky_tests(jenkins_url, job_name, num_previous_days):
  """
  Find any tests failures in the last 'num_previous_days'.
  Returns a list of TestFailure instances.
  """
  to_return = []
  # First list all builds
  builds = list_builds(jenkins_url, job_name)

  # Select only those in the last N days
  min_time = int(time.time()) - SECONDS_PER_DAY * num_previous_days
  builds = [b for b in builds if int(b['timestamp'])/1000 > min_time]

  # Filter out only those that failed
  failing_build_numbers = [b['number'] for b in builds if b['result'] == 'FAILURE']
  logging.debug("Recently failed builds: %s" % repr(failing_build_numbers))

  for build_number in failing_build_numbers:
    failing_subbuilds = get_failing_subbuilds(jenkins_url, job_name, build_number)
    logging.debug("build %d failing sub-builds: %s" % (build_number, repr(failing_subbuilds)))
    for failed_subbuild in failing_subbuilds:
      failing = find_failing_tests(failed_subbuild + "consoleText")
      if failing:
        logging.debug("Failed in build #%d: %s" % (build_number, repr(failing)))
        for test_name in failing:
          to_return.append(TestFailure(test_name, build_number, failed_subbuild))
  return to_return

def download_failure(failure, download_dir):
  test_output_url = "%s/artifact/build/test-logs/%s.txt.gz" % (failure.url, failure.test_name)
  req = urllib2.urlopen(test_output_url)
  out_path = os.path.join(download_dir,
      failure.test_name,
      "%s-%d.txt.gz" % (failure.test_name, failure.build_number))
  try:
    os.makedirs(os.path.dirname(out_path))
  except OSError:
    pass
  with open(out_path, "wb") as out:
    shutil.copyfileobj(req, out)

def main():
  logging.basicConfig(level=logging.INFO)
  opts = parse_args()
  all_failing = []
  for job in opts.job_names:
    all_failing.extend(find_flaky_tests(opts.jenkins_url, job, opts.days))

  # Group failures by test name
  by_test_name = defaultdict(lambda: [])
  for failure in all_failing:
    by_test_name[failure.test_name].append(failure)

  # Print a summary of failed tests
  print "Summary: %d test failures in last %d day(s)" % (len(all_failing), opts.days)
  print "Flaky tests:"
  for test_name, failed_builds in by_test_name.iteritems():
    print "  ", test_name, ":", ", ".join((str(x.build_number) for x in failed_builds))
    if opts.download:
      for b in failed_builds:
        download_failure(b, opts.download)
  if opts.download:
    print "Downloaded test failure logs into " + opts.download

if __name__ == "__main__":
  main()
