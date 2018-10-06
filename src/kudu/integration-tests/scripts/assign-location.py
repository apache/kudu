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

import argparse
import errno
import fcntl
import json
import time
import random

# This is a simple sequencer to be run as a location assignment script
# by a Kudu master. The script can be used in location-aware test scenarios
# and other cases when location assignment rules are specified simply as the
# distribution of tablet servers among locations: i.e. how many tablet
# servers should be in every specified location (see below for an example).
#
# The script takes as input location mapping rules and an identifier.
# On success, the script prints the location assigned to the specified
# identifier to stdout. The identifier might be any string uniquely identifying
# a tablet server.
#
# Locations are assigned based on:
#   a) Location mapping rules specified in the command line and sequencer's
#      offset persistently stored in a state file.
#   b) Previously established and persisted { id, location } mappings in the
#      state file.
#
# Once assigned, the location for the specified identifier is recorded and
# output again upon next call of the script for the same identifier.
#
# It's safe to run multiple instances of the script concurrently with the
# same set of parameters. The access to the sequencer's state file is
# serialized and the scripts produces consistent results for all concurrent
# callers.
#
# A location mapping rule is specified as a pair 'loc:num', where the 'num'
# stands for the number of servers to assign to the location 'loc'. Location
# mapping rules are provided to the script by --map 'loc:num' command line
# arguments.
#
# Below is an example of invocation of the script for location mapping rules
# specifying that location 'l0' should have one tablet server, location 'l1'
# should have  two, and location 'l2' should have three. The script is run
# to assign a location for a tablet server running at IP address 127.1.2.3.
#
#   assign-location.py --map l0:1 --map l1:2 --map l2:3 127.1.2.3
#

class LocationAssignmentRule(object):
  def __init__(self, location_mapping_rules):
    # Convert the input location information into an auxiliary array of
    # location strings.
    self.location_mapping_rules = location_mapping_rules
    if self.location_mapping_rules is None:
      self.location_mapping_rules = []
    self.locations = []
    self.total_count = 0

    seen_locations = []
    for info in self.location_mapping_rules:
      location, server_num_str = info.split(':')
      seen_locations.append(location)
      server_num = int(server_num_str)
      for i in range(0, server_num):
        self.total_count += 1
        self.locations.append(location)
    assert (len(set(seen_locations)) == len(seen_locations)), \
        'duplicate locations specified: {}'.format(seen_locations)

  def get_location(self, idx):
    """
    Get location for the specified index.
    """
    if self.locations:
      return self.locations[idx % len(self.locations)]
    else:
      return ""


def acquire_advisory_lock(fpath):
  """
  Acquire a lock on a special .lock file. Don't block while trying: return
  if failed to acquire a lock in 30 seconds.
  """
  timeout_seconds = 30
  now = time.clock()
  deadline = now + timeout_seconds
  random.seed(int(now))
  fpath_lock_file = fpath + ".lock"
  # Open the lock file; create the file if doesn't exist.
  lock_file = open(fpath_lock_file, 'w+')
  got_lock = False
  while time.clock() < deadline:
    try:
      fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
      got_lock = True
      break
    except IOError as e:
      if e.errno != errno.EAGAIN:
        raise
      else:
        time.sleep(random.uniform(0.001, 0.100))

  if not got_lock:
    raise Exception('could not obtain exclusive lock for {} in {} seconds',
        fpath_lock_file, timeout_seconds)

  return lock_file


def get_location(fpath, rule, uid, relaxed):
  """
  Return location for the specified identifier 'uid'. To do that, use the
  specified location mapping rules and the information stored
  in the sequencer's state file.

  * Obtain advisory lock for the state file (using additional .lock file)
  * If the sequencer's state file exists:
      1. Open the state file in read-only mode.
      2. Read the information from the state file and search for location
         assigned to the server with the specified identifier.
           a. If already assigned location found:
                -- Return the location.
           b. If location assigned to the identifier is not found:
                -- Use current sequence number 'seq' to assign next location
                   by calling LocationAssignmentRule.get_location(seq).
                -- Add the newly generated location assignment into the
                   sequencer's state.
                -- Increment the sequence number.
                -- Reopen the state file for writing (if file exists)
                -- Rewrite the file with the new state of the sequencer.
                -- Return the newly assigned location.
  * If the sequencer's state file does not exist:
      1. Set sequence number 'seq' to 0.
      2. Use current sequence number 'seq' to assign next location
         by calling LocationAssignmentRule.get_location(seq).
      3. Update the sequencer's state accordingly.
      3. Rewrite the file with the new state of the sequencer.
      4. Return the newly assigned location.
  """
  lock_file = acquire_advisory_lock(fpath)
  state_file = None
  try:
    state_file = open(fpath)
  except IOError as e:
    if e.errno != errno.ENOENT:
      raise

  new_assignment = False
  if state_file is None:
    seq = 0
    state = {}
    state['seq'] = seq
    state['mapping_rules'] = rule.location_mapping_rules
    state['mappings'] = {}
    mappings = state['mappings']
    new_assignment = True
  else:
    # If the file exists, it must have proper content.
    state = json.load(state_file)
    seq = state.get('seq')
    mapping_rules = state.get('mapping_rules')
    # Make sure the stored mapping rule corresponds to the specified in args.
    rule_stored = json.dumps(mapping_rules)
    rule_specified = json.dumps(rule.location_mapping_rules)
    if rule_stored != rule_specified:
      raise Exception('stored and specified mapping rules mismatch: '
                      '{} vs {}'.format(rule_stored, rule_specified))
    mappings = state['mappings']
    location = mappings.get(uid, None)
    if location is None:
      seq += 1
      state['seq'] = seq
      new_assignment = True

  if not new_assignment:
    return location

  if not relaxed and rule.total_count != 0 and rule.total_count <= seq:
    raise Exception('too many unique identifiers ({}) to assign next location '
                    'using mapping rules {}'.format(
                        seq + 1, rule.location_mapping_rules))

  if relaxed and rule.total_count <= seq:
    return ""

  # Get next location and add the { uid, location} binding into the mappings.
  location = rule.get_location(seq)
  mappings[uid] = location

  # Rewrite the file with the updated state information.
  if state_file is not None:
    state_file.close()
  state_file = open(fpath, 'w+')
  json.dump(state, state_file)
  state_file.close()
  lock_file.close()
  return location


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--state_store",
      nargs="?",
      default="/tmp/location-sequencer-state",
      help="path to a file to store the sequencer's state")
  parser.add_argument("--map", "-m",
      action="append",
      dest="location_mapping_rules",
      metavar="RULE",
      help="location mapping rule: number of tablet servers per specified "
      "location in form <location>:<number>; this option may be specified "
      "multiple times")
  parser.add_argument("--relaxed",
      action="store_true",
      help="whether to allow more location assignments than specified "
      "by the specified mapping rules")
  parser.add_argument("uid",
      help="hostname, IP address, or any other unique identifier")
  args = parser.parse_args()

  location = get_location(args.state_store,
      LocationAssignmentRule(args.location_mapping_rules), args.uid, args.relaxed)
  print(location)


if __name__ == "__main__":
  main()
