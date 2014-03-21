#!/usr/bin/env python
# Copyright (c) 2014, Cloudera, inc.
#
# Script to look at recent Jenkins history and find any unit tests which have
# recently failed. Outputs a list of these tests on stdout.

import logging
from optparse import OptionParser
import re
import simplejson
import time
import urllib2

# Configuration
DEFAULT_JENKINS_URL = "http://sandbox.jenkins.cloudera.com"
DEFAULT_JOB_NAME = "kudu-test"
NUM_PREVIOUS_DAYS = 14

# Constants
SECONDS_PER_DAY = 186400
FAILED_TEST_RE = re.compile('Test.*: (\S+).*\*\*\*Failed')

def parse_args():
  parser = OptionParser()
  parser.add_option("-J", "--jenkins-url", type="string",
                   help="Jenkins URL", default=DEFAULT_JENKINS_URL)
  parser.add_option("-j", "--job-name", type="string", action="append",
                    dest="job_names", help="Job name to look at (may specify multiple times)",
                    default=[DEFAULT_JOB_NAME])
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

def find_flaky_tests(jenkins_url, job_name):
  all_failing = set()
  # First list all builds
  builds = list_builds(jenkins_url, job_name)

  # Select only those in the last N days
  min_time = int(time.time()) - SECONDS_PER_DAY * NUM_PREVIOUS_DAYS
  builds = [b for b in builds if b['timestamp'] > min_time]

  # Filter out only those that failed
  failing_build_numbers = [b['number'] for b in builds if b['result'] == 'FAILURE']
  logging.info("Recently failed builds: %s" % repr(failing_build_numbers))

  for build_number in failing_build_numbers:
    failing_subbuilds = get_failing_subbuilds(jenkins_url, job_name, build_number)
    logging.info("build %d failing sub-builds: %s" % (build_number, repr(failing_subbuilds)))
    for failed_subbuild in failing_subbuilds:
      failing = find_failing_tests(failed_subbuild + "consoleText")
      if failing:
        logging.info("Failed in build #%d: %s" % (build_number, repr(failing)))
        all_failing.update(failing)
  return all_failing

def main():
  logging.basicConfig(level=logging.INFO)
  opts = parse_args()
  all_failing = set()
  for job in opts.job_names:
    all_failing.update(find_flaky_tests(opts.jenkins_url, job))
  print "\n".join(all_failing)

if __name__ == "__main__":
  main()
