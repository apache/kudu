//  Copyright (c) 2014, Cloudera, inc.
//  Confidential Cloudera Information: Covered by NDA.
//
//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <string>
#include <tr1/unordered_set>
#include <tr1/unordered_map>
#include <vector>

#include "kudu/util/condition_variable.h"
#include "kudu/util/mutex.h"

#ifdef NDEBUG
#define TEST_SYNC_POINT(x)
#else

namespace kudu {

// This class provides facility to reproduce race conditions deterministically
// in unit tests.
// Developer could specify sync points in the codebase via TEST_SYNC_POINT.
// Each sync point represents a position in the execution stream of a thread.
// In the unit test, 'Happens After' relationship among sync points could be
// setup via SyncPoint::LoadDependency, to reproduce a desired interleave of
// threads execution.

class SyncPoint {
 public:
  static SyncPoint* GetInstance();

  struct Dependency {
    Dependency(const std::string& predecessor, const std::string& successor);

    std::string predecessor_;
    std::string successor_;
  };
  // call once at the beginning of a test to setup the dependency between
  // sync points
  void LoadDependency(const std::vector<Dependency>& dependencies);

  // enable sync point processing (disabled on startup)
  void EnableProcessing();

  // disable sync point processing
  void DisableProcessing();

  // remove the execution trace of all sync points
  void ClearTrace();

  // triggered by TEST_SYNC_POINT, blocking execution until all predecessors
  // are executed.
  void Process(const std::string& point);

  // TODO: it might be useful to provide a function that blocks until all
  // sync points are cleared.

 private:
  SyncPoint();

  bool PredecessorsAllCleared(const std::string& point);

  // successor/predecessor map loaded from LoadDependency
  std::tr1::unordered_map<std::string, std::vector<std::string> > successors_;
  std::tr1::unordered_map<std::string, std::vector<std::string> > predecessors_;

  Mutex mutex_;
  ConditionVariable cv_;
  // sync points that have been passed through
  std::tr1::unordered_set<std::string> cleared_points_;
  bool enabled_;
};

}  // namespace kudu

// Use TEST_SYNC_POINT to specify sync points inside code base.
// Sync points can have happens-after depedency on other sync points,
// configured at runtime via SyncPoint::LoadDependency. This could be
// utilized to re-produce race conditions between threads.
// TEST_SYNC_POINT is no op in release build.
#define TEST_SYNC_POINT(x) kudu::SyncPoint::GetInstance()->Process(x)
#endif  // NDEBUG
