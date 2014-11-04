#!/usr/bin/env python
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# This script parses a test log (provided on stdin) and returns
# a summary of the error which caused the test to fail.

import re
import sys

# Read at most 100MB of a test log.
# Rarely would this be exceeded, but we don't want to end up
# swapping, etc.
MAX_MEMORY = 100 * 1024 * 1024

START_TESTCASE_RE = re.compile(r'\[ RUN\s+\] (.+)$')
END_TESTCASE_RE = re.compile(r'\[\s+(?:OK|FAILED)\s+\] (.+)$')
ASAN_ERROR_RE = re.compile('ERROR: AddressSanitizer')
TSAN_ERROR_RE = re.compile('WARNING: ThreadSanitizer.*')
END_TSAN_ERROR_RE = re.compile('SUMMARY: ThreadSanitizer.*')
FATAL_LOG_RE = re.compile(r'^F\d\d\d\d \d\d:\d\d:\d\d\.\d\d\d\d\d\d\s+\d+ (.*)')
LEAK_CHECK_SUMMARY_RE = re.compile('Leak check.*detected leaks')
LINE_RE = re.compile(r"^.*$", re.MULTILINE)
STACKTRACE_ELEM_RE = re.compile(r'^    @')
IGNORED_STACKTRACE_ELEM_RE = re.compile(
  r'(google::logging|google::LogMessage|\(unknown\)| testing::)')
TEST_FAILURE_RE = re.compile(r'.*\d+: Failure$')
GLOG_LINE_RE = re.compile(r'^[WIEF]\d\d\d\d \d\d:\d\d:\d\d')

def consume_rest(line_iter):
  """ Consume and return the rest of the lines in the iterator. """
  return [l.group(0) for l in line_iter]

def consume_until(line_iter, end_re):
  """
  Consume and return lines from the iterator until one matches 'end_re'.
  The line matching 'end_re' will not be returned, but will be consumed.
  """
  ret = []
  for l in line_iter:
    line = l.group(0)
    if end_re.search(line):
      break
    ret.append(line)
  return ret

def remove_glog_lines(lines):
  """ Remove any lines from the list of strings which appear to be GLog messages. """
  return [l for l in lines if not GLOG_LINE_RE.search(l)]

def extract_failure_summary(log_text):
  cur_test_case = None
  errors = list()

  # Iterate over the lines, using finditer instead of .split()
  # so that we don't end up doubling memory usage.
  line_iter = LINE_RE.finditer(log_text)
  for match in line_iter:
    line = match.group(0)

    # Track the currently-running test case
    m = START_TESTCASE_RE.search(line)
    if m:
      cur_test_case = m.group(1)
    m = END_TESTCASE_RE.search(line)
    if m:
      cur_test_case = None

    # Look for ASAN errors.
    m = ASAN_ERROR_RE.search(line)
    if m:
      error_signature = cur_test_case + ": " + line + "\n"
      asan_lines = remove_glog_lines(consume_rest(line_iter))
      error_signature += "\n".join(asan_lines)
      errors.append(error_signature)

    # Look for TSAN errors
    m = TSAN_ERROR_RE.search(line)
    if m:
      error_signature = m.group(0)
      error_signature += "\n".join(remove_glog_lines(
          consume_until(line_iter, END_TSAN_ERROR_RE)))
      errors.append(cur_test_case + ": " + error_signature)

    # Look for test failures
    m = TEST_FAILURE_RE.search(line)
    if m:
      error_signature = m.group(0) + "\n"
      error_signature += "\n".join(remove_glog_lines(
          consume_until(line_iter, END_TESTCASE_RE)))
      errors.append(cur_test_case + ": " + error_signature)

    # Look for fatal log messages (including CHECK failures)
    m = FATAL_LOG_RE.search(line)
    if m:
      error_signature = cur_test_case + ": " + m.group(1) + "\n"
      remaining_lines = consume_rest(line_iter)
      remaining_lines = [l for l in remaining_lines if STACKTRACE_ELEM_RE.search(l)
                         and not IGNORED_STACKTRACE_ELEM_RE.search(l)]
      error_signature += "\n".join(remaining_lines)
      errors.append(error_signature)

    # Look for leak check summary (comes at the end of a log, not part of a single test)
    m = LEAK_CHECK_SUMMARY_RE.search(line)
    if m:
      error_signature = "Memory leak\n"
      error_signature += "\n".join(consume_rest(line_iter))
      errors.append(error_signature)

  return "\n\n".join(errors)

def main():
  log_text = sys.stdin.read(MAX_MEMORY)
  summary = extract_failure_summary(log_text)
  print summary

if __name__ == "__main__":
  main()
