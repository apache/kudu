#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# This script parses a test log (provided on stdin) and returns
# a summary of the error which caused the test to fail.

from xml.sax.saxutils import quoteattr
import argparse
import os
import re
import sys
import unittest

# Read at most 100MB of a test log.
# Rarely would this be exceeded, but we don't want to end up
# swapping, etc.
MAX_MEMORY = 100 * 1024 * 1024

START_TESTCASE_RE = re.compile(r'\[ RUN\s+\] (.+)$')
END_TESTCASE_RE = re.compile(r'\[\s+(?:OK|FAILED)\s+\] (.+)$')
ASAN_ERROR_RE = re.compile('ERROR: (AddressSanitizer|LeakSanitizer)')
TSAN_ERROR_RE = re.compile('WARNING: ThreadSanitizer.*')
END_TSAN_ERROR_RE = re.compile('SUMMARY: ThreadSanitizer.*')
UBSAN_ERROR_RE = re.compile(r'SUMMARY: UndefinedBehaviorSanitizer')
FATAL_LOG_RE = re.compile(r'^F\d\d\d\d \d\d:\d\d:\d\d\.\d\d\d\d\d\d\s+\d+ (.*)')
LINE_RE = re.compile(r"^.*$", re.MULTILINE)
STACKTRACE_ELEM_RE = re.compile(r'^    @')
IGNORED_STACKTRACE_ELEM_RE = re.compile(
  r'(google::logging|google::LogMessage|\(unknown\)| testing::)')
TEST_FAILURE_RE = re.compile(r'.*\d+: Failure$')
GLOG_LINE_RE = re.compile(r'^[WIEF]\d\d\d\d \d\d:\d\d:\d\d')


class ParsedTest(object):
  """
  The LogParser creates one instance of this class for each test that is discovered
  while parsing the log.
  """
  def __init__(self, test_name):
    self.test_name = test_name
    self.errors = []


class LogParser(object):
  """
  Parser for textual gtest output
  """
  def __init__(self):
    self._tests = []
    self._cur_test = None

  @staticmethod
  def _consume_rest(line_iter):
    """ Consume and return the rest of the lines in the iterator. """
    return [l for l in line_iter]

  @staticmethod
  def _consume_until(line_iter, end_re):
    """
    Consume and return lines from the iterator until one matches 'end_re'.
    The line matching 'end_re' will not be returned, but will be consumed.
    """
    ret = []
    for line in line_iter:
      if end_re.search(line):
        break
      ret.append(line)
    return ret

  @staticmethod
  def _remove_glog_lines(lines):
    """ Remove any lines from the list of strings which appear to be GLog messages. """
    return [l for l in lines if not GLOG_LINE_RE.search(l)]

  def _record_error(self, error):
    if self._cur_test is None:
      # TODO(todd) would be nice to have a more specific name indicating which
      # test file caused the issue.
      self._start_test("General.OutsideOfAnyTestCase")
      self._record_error(error)
      self._end_test()
    else:
      self._cur_test.errors.append(error)

  def _start_test(self, test_name):
    assert test_name is not None
    self._cur_test = ParsedTest(test_name)
    self._tests.append(self._cur_test)

  def _end_test(self):
    self._cur_test = None

  @staticmethod
  def _fast_re(substr, regexp, line):
    """
    Implements a micro-optimization: returns true if 'line' matches 'regexp,
    but short-circuited by a much faster check whether 'line' contains 'substr'.
    This provides a big speed-up since substring searches execute much more
    quickly than regexp matches.
    """
    if substr not in line:
      return None
    return regexp.search(line)

  def parse_text(self, log_text):
    # Iterate over the lines, using finditer instead of .split()
    # so that we don't end up doubling memory usage.
    def line_iter():
      for match in LINE_RE.finditer(log_text):
        yield match.group(0)
    self.parse_lines(line_iter())

  def parse_lines(self, line_iter):
    """
    Arguments:
      lines: generator which should yield lines of log output
    """
    for line in line_iter:
      # Track the currently-running test case
      m = self._fast_re('RUN', START_TESTCASE_RE, line)
      if m:
        self._start_test(m.group(1))
        continue

      m = self._fast_re('[', END_TESTCASE_RE, line)
      if m:
        self._end_test()
        continue

      # Look for ASAN errors.
      m = self._fast_re('ERROR', ASAN_ERROR_RE, line)
      if m:
        error_signature = line + "\n"
        # ASAN errors kill the process, so we consume the rest of the log
        # and remove any lines that don't look like part of the stack trace
        asan_lines = self._remove_glog_lines(self._consume_rest(line_iter))
        error_signature += "\n".join(asan_lines)
        self._record_error(error_signature)
        continue

      # Look for TSAN errors
      m = self._fast_re('ThreadSanitizer', TSAN_ERROR_RE, line)
      if m:
        error_signature = m.group(0)
        error_signature += "\n".join(self._remove_glog_lines(
          self._consume_until(line_iter, END_TSAN_ERROR_RE)))
        self._record_error(error_signature)
        continue

      # Look for UBSAN errors
      m = self._fast_re('UndefinedBehavior', UBSAN_ERROR_RE, line)
      if m:
        # UBSAN errors are a single line.
        # TODO: there is actually some info on the previous line but there
        # is no obvious prefix to look for.
        self._record_error(line)
        continue

      # Look for test failures
      # - slight micro-optimization to check for substring before running the regex
      m = self._fast_re('Failure', TEST_FAILURE_RE, line)
      if m:
        error_signature = m.group(0) + "\n"
        error_signature += "\n".join(self._remove_glog_lines(
          self._consume_until(line_iter, END_TESTCASE_RE)))
        self._record_error(error_signature)
        self._end_test()
        continue

      # Look for fatal log messages (including CHECK failures)
      # - slight micro-optimization to check for 'F' before running the regex
      m = line and line[0] == 'F' and FATAL_LOG_RE.search(line)
      if m:
        error_signature = m.group(1) + "\n"
        remaining_lines = self._consume_rest(line_iter)
        remaining_lines = [l for l in remaining_lines if STACKTRACE_ELEM_RE.search(l) and
                           not IGNORED_STACKTRACE_ELEM_RE.search(l)]
        error_signature += "\n".join(remaining_lines)
        self._record_error(error_signature)

    # Sometimes we see crashes that the script doesn't know how to parse.
    # When that happens, we leave a generic message to be picked up by Jenkins.
    if self._cur_test and not self._cur_test.errors:
      self._record_error("Unrecognized error type. Please see the error log for more information.")

  # Return failure summary formatted as text.
  def text_failure_summary(self):
    msgs = []
    for test in self._tests:
      for error in test.errors:
        msgs.append("%s: %s\n" % (test.test_name, error))

    return "\n".join(msgs)

  def xml_failure_summary(self):
    # Example format:
    """
    <testsuites>
      <testsuite name="ClientTest">
        <testcase name="TestReplicatedMultiTabletTableFailover" classname="ClientTest">
          <error message="Check failed: ABC != XYZ">
            <![CDATA[ ... stack trace ... ]]>
          </error>
        </testcase>
      </testsuite>
    </testsuites>
    """
    ret = ""

    cur_test_suite = None
    ret += '<testsuites>\n'

    found_test_suites = False
    for test in self._tests:
      if not test.errors:
        continue

      (test_suite, test_case) = test.test_name.split(".")

      # Test suite initialization or name change.
      if test_suite and test_suite != cur_test_suite:
        if cur_test_suite:
          ret += '  </testsuite>\n'
        cur_test_suite = test_suite
        ret += '  <testsuite name="%s">\n' % cur_test_suite
        found_test_suites = True

      # Print each test case.
      ret += '    <testcase name="%s" classname="%s">\n' % (test_case, cur_test_suite)
      errors = "\n\n".join(test.errors)
      first_line = re.sub("\n.*", '', errors)
      ret += '      <error message=%s>\n' % quoteattr(first_line)
      ret += '<![CDATA[\n'
      ret += errors
      ret += ']]>\n'
      ret += '      </error>\n'
      ret += '    </testcase>\n'

    if found_test_suites:
      ret += '  </testsuite>\n'
    ret += '</testsuites>\n'
    return ret


# Parse log lines and return failure summary formatted as text.
#
# Print failure summary based on desired output format.
# 'tests' is a list of all tests run (in order), not just the failed ones.
# This allows us to print the test results in the order they were run.
# 'errors_by_test' is a dict of lists, keyed by test name.
#
# This helper function is part of a public API called from test_result_server.py
def extract_failure_summary(log_text, format='text'):
  p = LogParser()
  p.parse_text(log_text)
  if format == 'text':
    return p.text_failure_summary()
  else:
    return p.xml_failure_summary()


class Test(unittest.TestCase):
  _TEST_DIR = os.path.join(os.path.dirname(__file__), "build-support-test-data")

  def __init__(self, *args, **kwargs):
    super(Test, self).__init__(*args, **kwargs)
    self.regenerate = os.environ.get('REGENERATE_TEST_EXPECTATIONS') == '1'

  def test_all(self):
    for child in os.listdir(self._TEST_DIR):
      if not child.endswith(".txt") or '-out' in child:
        continue
      base, _ = os.path.splitext(child)

      p = LogParser()
      p.parse_text(file(os.path.join(self._TEST_DIR, child)).read())
      self._do_test(p.text_failure_summary(), base + "-out.txt")
      self._do_test(p.xml_failure_summary(), base + "-out.xml")

  def _do_test(self, got_value, filename):
    path = os.path.join(self._TEST_DIR, filename)
    if self.regenerate:
      print("Regenerating %s" % path)
      with file(path, "w") as f:
        f.write(got_value)
    else:
      self.assertEquals(got_value, file(path).read())


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("-x", "--xml",
                      help="Print output in JUnit report XML format (default: plain text)",
                      action="store_true")
  parser.add_argument("path", nargs="?", help="File to parse. If not provided, parses stdin")
  args = parser.parse_args()

  if args.path:
    in_file = file(args.path)
  else:
    in_file = sys.stdin
  log_text = in_file.read(MAX_MEMORY)
  format = args.xml and 'xml' or 'text'
  sys.stdout.write(extract_failure_summary(log_text, format))


if __name__ == "__main__":
  main()
