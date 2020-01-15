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

from collections import Counter
from functools import total_ordering
import glob
import gzip
import heapq
import io
import logging
import random
import time
import unittest
try:
  xrange  # For Python 2
except NameError:
  xrange = range  # For Python 3

# Adjustable experiment parameters.
BLOCK_SIZE = 1000
MAX_ITEM = 1000000
NUM_ITERATORS = 100
MIN_ITEMS_PER_ITERATOR = 1
MAX_ITEMS_PER_ITERATOR = 10000

class MyHeap(object):
  """ Heap with custom key comparator. See https://stackoverflow.com/a/8875823. """
  def __init__(self, initial=None, key=lambda x:x):
    self.key = key
    if initial:
      self._data = [(key(item), item) for item in initial]
      heapq.heapify(self._data)
    else:
      self._data = []

  def push(self, item):
    heapq.heappush(self._data, (self.key(item), item))

  def pop(self):
    return heapq.heappop(self._data)[1]

  def top(self):
    return self._data[0][1]

  def __len__(self):
    return len(self._data)

  def __str__(self):
    return ', '.join(str(e[0]) for e in self._data)

class BlockIterator(object):
  """ Iterator of generic items. Returns items on a block basis. """
  next_block_idx = 0

  def __init__(self, items):
    assert len(items) > 0
    self.items = items
    self.lower_bound = self.items[0]
    self.upper_bound = self.items[-1]

  def next_block(self):
    """ Return an array containing the next block of items """
    block = self.items[self.next_block_idx:self.next_block_idx + BLOCK_SIZE]
    assert len(block) > 0
    self.next_block_idx += len(block)
    return block

  def has_next(self):
    return len(self.items) > 0 and self.next_block_idx < len(self.items)

  def __str__(self):
    return ", ".join(str(i) for i in self.items[self.next_block_idx:])

class FileBlockIterator(object):
  """
  Iterator of generic items loaded from an (optionally gzipped) file. Returns
  items on a block basis.

  When constructed for a file with name 'foo', expects to find a file with name
  'foo.firstlast' containing two lines: the first and last items in 'foo'.
  """

  def __init__(self, filename, stats):
    self.stats = stats
    self.filename = filename

    if self.filename.endswith(".gz"):
      self.fh = gzip.open(self.filename, "rb")
    else:
      self.fh = open(self.filename, "rb")
    self.br = io.BufferedReader(self.fh)

    with open(self.filename + ".firstlast", "rb") as flfh:
      l = flfh.readline()[:-1]
      assert len(l) > 0
      self.lower_bound = ComparisonCountingObject(l, self.stats)
      l = flfh.readline()[:-1]
      assert len(l) > 0
      self.upper_bound = ComparisonCountingObject(l, self.stats)

    self.next_line = self.br.readline()

  def next_block(self):
    """ Return an array containing the next block of items """
    assert self.has_next()
    block = []
    while True:
      l = self.next()
      block.append(ComparisonCountingObject(l, self.stats))
      if not self.has_next():
        # EOF
        break
      if len(block) == BLOCK_SIZE:
        break
    return block

  def next(self):
    assert self.has_next()
    l = self.next_line
    self.next_line = self.br.readline()
    return l[:-1]

  def has_next(self):
    return len(self.next_line) > 0

  def __str__(self):
    return self.filename

class PagingBlockIterator(object):
  """
  Iterator-like object that pages an entire block of items into memory and
  provides finer-grained access to the items.
  """

  def __init__(self, block_iter, stats):
    """
    block_iter: BlockIterator
    stats: collections.Counter for keeping track of perf stats
    """
    self.block_iter = block_iter
    self.stats = stats
    self.paged_block = None
    self.paged_block_idx = -1
    self.paged_one_block = False

  def page_next_block(self):
    # Block must be fully consumed before paging the next one.
    assert not self.paged_block or len(self.paged_block) == self.paged_block_idx + 1

    if self.block_iter.has_next():
      next_block = self.block_iter.next_block()
      assert len(next_block) > 0

      if self.paged_block is None:
        self.stats['blocks_in_mem'] += 1
        self.stats['peak_blocks_in_mem'] = max(self.stats['peak_blocks_in_mem'],
                                               self.stats['blocks_in_mem'])

      self.paged_block = next_block
      self.paged_block_idx = 0
      self.paged_one_block = True
    else:
      if self.paged_block is not None:
        self.stats['blocks_in_mem'] -= 1
      self.paged_block = None

  def advance(self):
    assert self.paged_block

    item = self.paged_block[self.paged_block_idx]
    if len(self.paged_block) == self.paged_block_idx + 1:
      self.page_next_block()
      return True
    else:
      self.paged_block_idx += 1
      return False

  def has_next(self):
    return self.paged_block or self.block_iter.has_next()

  def has_ever_paged(self):
    return self.paged_one_block

  def min(self):
    return (self.paged_block[0]
            if self.has_ever_paged() else self.block_iter.lower_bound)

  def max(self):
    return (self.paged_block[-1]
            if self.has_ever_paged() else self.block_iter.upper_bound)

  def cur(self):
    return (self.paged_block[self.paged_block_idx]
            if self.has_ever_paged() else self.block_iter.lower_bound)

  def __str__(self):
    s = ""
    if self.paged_block:
      s += ", ".join(str(i) for i in self.paged_block[self.paged_block_idx:])
    suffix = str(self.block_iter)
    if suffix:
      s += ", "
      s += suffix
    return s

def remove_dead_iters(iters):
  """
  Convenience method to filter out any fully-consumed iterators.
  """
  live_iters = []
  for i in iters:
    if i.has_next():
      live_iters.append(i)
  return live_iters

class NaiveMergeIterator(object):
  """
  Simple merge iterator that uses no optimizations whatsoever. Every call to
  next() iterates over all live iterators and returns the smallest item.
  """
  def __init__(self, iters):
    self.iters = remove_dead_iters(iters)

    # This iterator ignores bounds, so we must page in all blocks up front.
    [i.page_next_block() for i in self.iters]

  def next(self):
    smallest_iter = None
    for i in self.iters:
      if not smallest_iter or i.cur() < smallest_iter.cur():
        smallest_iter = i
    assert smallest_iter
    item = smallest_iter.cur()
    smallest_iter.advance()
    self.iters = remove_dead_iters(self.iters)
    return item

  def has_next(self):
    return len(self.iters) > 0

class SingleHeapMergeIterator(object):
  """
  More sophisticated merge iterator that uses a heap to optimize next() calls.

  Initially, the underlying iterators' bounds are used to establish heap order.
  When an iterator is next (i.e. when its first item is the global minimum), the
  first block is paged in and the iterator is put back in the heap. In the
  steady state, blocks are paged in as iterators are advanced and the heap is
  reordered with every call to next().
  """
  def __init__(self, iters):
    self.iters = MyHeap(remove_dead_iters(iters), key=lambda x : x.cur())

  def next(self):
    smallest_iter = None
    while True:
      smallest_iter = self.iters.pop()
      if not smallest_iter.has_ever_paged():
        # Page in the first block and retry.
        smallest_iter.page_next_block()
        self.iters.push(smallest_iter)
        continue
      break

    item = smallest_iter.cur()
    smallest_iter.advance()
    if smallest_iter.has_next():
      self.iters.push(smallest_iter)
    return item

  def has_next(self):
    return len(self.iters) > 0

class DoubleHeapMergeIterator(object):
  """
  Hot/cold heap-based merge iterator.

  This variant assigns iterators to two heaps. The "hot" heap includes all
  iterators currently needed to perform the merge, while the "cold" heap
  contains the rest.

  While algorithmically equivalent to the basic heap-based merge iterator, the
  amount of heap reordering is typically less due to the reduced size of the
  working set (i.e. the size of the hot heap). This is especially true when the
  input iterators do not overlap, as that allows the algorithm to maximize the
  size of the cold heap.
  """
  def __init__(self, iters):
    self.cold = MyHeap(remove_dead_iters(iters), key=lambda x : x.cur())
    self.hot = MyHeap([], key=lambda x : x.cur())
    self._refill_hot()

  def _refill_hot(self):
    while len(self.cold) > 0 and (len(self.hot) == 0 or
                                  self.hot.top().max() >= self.cold.top().cur()):
      warmest = self.cold.pop()
      if not warmest.has_ever_paged():
        # Page in the first block and retry.
        warmest.page_next_block()
        self.cold.push(warmest)
        continue
      self.hot.push(warmest)

  def next(self):
    smallest_iter = self.hot.pop()

    item = smallest_iter.cur()
    paged_new_block = smallest_iter.advance()
    is_dead = not smallest_iter.has_next()

    if is_dead:
      self._refill_hot()
    elif paged_new_block:
      if len(self.hot) > 0 and self.hot.top().max() < smallest_iter.cur():
        # 'smallest_iter' is no longer in the merge window.
        self.cold.push(smallest_iter)
      else:
        self.hot.push(smallest_iter)
      self._refill_hot()
    else:
      self.hot.push(smallest_iter)
    return item

  def has_next(self):
    return len(self.hot) > 0 or len(self.cold) > 0

class TripleHeapMergeIterator(object):
  """
  Advanced hot/cold heap-based merge iterator.

  Like DoubleHeapMergeIterator but uses an additional heap (of the result of
  max() in each iterator found in "hot") to more accurately track the top end
  of the merge window. The result is an even smaller hot heap.
  """
  def __init__(self, iters):
    self.cold = MyHeap(remove_dead_iters(iters), key=lambda x : x.cur())
    self.hot = MyHeap([], key=lambda x : x.cur())
    self.hotmaxes = MyHeap([])
    self._refill_hot()

  def _refill_hot(self):
    while len(self.cold) > 0 and (len(self.hotmaxes) == 0 or
                                  self.hotmaxes.top() >= self.cold.top().cur()):
      warmest = self.cold.pop()
      if not warmest.has_ever_paged():
        # Page in the first block and retry.
        warmest.page_next_block()
        self.cold.push(warmest)
        continue
      self.hot.push(warmest)
      self.hotmaxes.push(warmest.max())

  def next(self):
    smallest_iter = self.hot.pop()
    # Defer pop of hotmaxes; it only needs to happen if we've finished a block.

    item = smallest_iter.cur()
    paged_new_block = smallest_iter.advance()
    is_dead = not smallest_iter.has_next()

    if is_dead:
      self.hotmaxes.pop()
      self._refill_hot()
    elif paged_new_block:
      self.hotmaxes.pop()
      if len(self.hotmaxes) > 0 and self.hotmaxes.top() < smallest_iter.cur():
        # 'smallest_iter' is no longer in the merge window.
        self.cold.push(smallest_iter)
      else:
        self.hot.push(smallest_iter)
        self.hotmaxes.push(smallest_iter.max())
      self._refill_hot()
    else:
      self.hot.push(smallest_iter)
    return item

  def has_next(self):
    return len(self.hot) > 0 or len(self.cold) > 0

@total_ordering
class ComparisonCountingObject(object):
  def __init__(self, val, stats):
    self.val = val
    self.stats = stats

  def __eq__(self, rhs):
    assert isinstance(rhs, ComparisonCountingObject)
    self.stats['cmp'] += 1
    return self.val.__eq__(rhs.val)

  def __lt__(self, rhs):
    assert isinstance(rhs, ComparisonCountingObject)
    self.stats['cmp'] += 1
    return self.val.__lt__(rhs.val)

  def __str__(self):
    return str(self.val)

class ComparisonCountingInt(object):
  def __init__(self, val, stats):
    self.val = val
    self.stats = stats

  def __cmp__(self, rhs):
    assert isinstance(rhs, ComparisonCountingInt)
    self.stats['cmp'] += 1
    return self.val.__cmp__(rhs.val)

  def __str__(self):
    return str(self.val)

class TestMerges(unittest.TestCase):
  maxDiff = 1e9
  def generate_input(self, pattern):
    lists_of_items = []
    expected_items = []
    last_item = 0
    for i in xrange(NUM_ITERATORS):
      if pattern == 'overlapping':
        min_item = 0
      elif pattern == 'non-overlapping':
        min_item = last_item
      elif pattern == 'half-overlapping':
        min_item = last_item - MAX_ITEM / 2
      num_items = random.randint(MIN_ITEMS_PER_ITERATOR, MAX_ITEMS_PER_ITERATOR)
      items = random.sample(xrange(min_item, MAX_ITEM + min_item), num_items)
      items.sort()
      lists_of_items.append(items)
      expected_items.extend(items)
      last_item = items[-1]
    expected_items.sort()
    return (lists_of_items, expected_items)

  def run_merge(self, merge_type, pattern, lists_of_items, list_of_files, expected_results):
    stats = Counter()
    start = time.time()
    if lists_of_items:
      iters = [PagingBlockIterator(BlockIterator([ComparisonCountingInt(i, stats) for i in l]),
                                   stats) for l in lists_of_items]
    else:
      assert list_of_files
      iters = [PagingBlockIterator(FileBlockIterator(f, stats),
                                   stats) for f in list_of_files]
    logging.info("Starting merge with {}".format(merge_type.__name__))
    merge_iter = merge_type(iters)
    logging.info("Initialized iterator")
    results = []
    num_results = 0
    t1 = start
    while merge_iter.has_next():
      n = merge_iter.next().val
      num_results += 1
      if expected_results:
        results.append(n)

      t2 = time.time()
      if t2 - t1 > 10:
        logging.info("Merged {} elements ({} eps) {}".format(
          num_results,
          num_results / (t2 - start),
          repr(stats)))
        t1 = t2
    elapsed = time.time() - start
    logging.info("Merged {} elements".format(num_results))
    if expected_results:
      self.assertEqual(expected_results, results)
    logging.info("{} with {} input: {}s {}".format(
      merge_type.__name__,
      pattern,
      elapsed,
      repr(stats)))

  def _do_test(self, pattern):
    lists_of_items, expected_items = self.generate_input(pattern)

    # Commented out because it's too slow with the current parameters.
    #
    # self.run_merge(NaiveMergeIterator, pattern, lists_of_items, None, expected_items)
    self.run_merge(SingleHeapMergeIterator, pattern, lists_of_items, None, expected_items)
    self.run_merge(DoubleHeapMergeIterator, pattern, lists_of_items, None, expected_items)
    self.run_merge(TripleHeapMergeIterator, pattern, lists_of_items, None, expected_items)

  def test_overlapping_input(self):
    self._do_test('overlapping')

  def test_nonoverlapping_input(self):
    self._do_test('non-overlapping')

  def test_half_overlapping_input(self):
    self._do_test('half-overlapping')

  def test_real_input(self):
    list_of_files = glob.glob("rowset_keys_*.gz")
    if len(list_of_files) == 0:
      self.skipTest("No real input found")

    # Commented out because it's too slow with the current parameters.
    #
    # self.run_merge(NaiveMergeIterator, "real", None, list_of_files, None)
    self.run_merge(SingleHeapMergeIterator, "real", None, list_of_files, None)
    self.run_merge(DoubleHeapMergeIterator, "real", None, list_of_files, None)
    self.run_merge(TripleHeapMergeIterator, "real", None, list_of_files, None)

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO,
                      format='%(asctime)s %(levelname)s: %(message)s')
  unittest.main()
