// Copyright 2010 Google Inc.  All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "gutil/exception/coowned_pointer.h"

#include <gtest/gtest.h>
#include "gutil/scoped_ptr.h"

namespace common {

// Helper class w/ external liveness test.
class Target {
 public:
  Target(bool* alive) : alive_(alive) { *alive_ = true; }
  ~Target() { *alive_ = false; }
 private:
  bool* alive_;
};

TEST(CoownedPointer, NullWorks) {
  CoownedPointer<Target> p(NULL);
}

TEST(CoownedPointer, NullWithCopyingWorks) {
  CoownedPointer<Target> p1(NULL);
  CoownedPointer<Target> p2(p1);
  {
    CoownedPointer<Target> p3(p1);
    CoownedPointer<Target> p4(p2);
  }
  CoownedPointer<Target> p5(p2);
}

TEST(CoownedPointer, NullWithAssigningWorks) {
  CoownedPointer<Target> p1(NULL);
  CoownedPointer<Target> p2;
  p2 = p1;
  {
    CoownedPointer<Target> p3(p1);
    p3 = p2;
    CoownedPointer<Target> p4(p2);
    p4 = p3;
  }
  CoownedPointer<Target> p5(p2);
  CoownedPointer<Target>& p6(p5);
  p5 = p6;  // Testing self-assignment.
  p5 = p2;
}

TEST(CoownedPointer, DataWorks) {
  bool alive;
  {
    CoownedPointer<Target> p(new Target(&alive));
    EXPECT_TRUE(alive);
  }
  EXPECT_FALSE(alive);
}

TEST(CoownedPointer, DataWithCopyingWorks) {
  bool alive;
  {
    CoownedPointer<Target> p1(new Target(&alive));
    CoownedPointer<Target> p2(p1);
    EXPECT_TRUE(alive);
    {
      CoownedPointer<Target> p3(p1);
      CoownedPointer<Target> p4(p2);
      EXPECT_TRUE(alive);
    }
    CoownedPointer<Target> p5(p2);
    EXPECT_TRUE(alive);
  }
  EXPECT_FALSE(alive);
}

TEST(CoownedPointer, DataWithAssigningWorks) {
  bool alive;
  {
    CoownedPointer<Target> p1(new Target(&alive));
    CoownedPointer<Target> p2;
    EXPECT_TRUE(alive);
    p2 = p1;
    EXPECT_TRUE(alive);
    {
      CoownedPointer<Target> p3(p1);
      p3 = p2;
      CoownedPointer<Target> p4(p2);
      p4 = p3;
      EXPECT_TRUE(alive);
    }
    CoownedPointer<Target> p5(p2);
    CoownedPointer<Target>& p6(p5);
    p5 = p6;  // Testing self-assignment.
    p5 = p2;
    EXPECT_TRUE(alive);
  }
  EXPECT_FALSE(alive);
}

TEST(CoownedPointer, ReassignmentDeletes) {
  bool alive;
  CoownedPointer<Target> p1(new Target(&alive));
  CoownedPointer<Target> p2 = p1;
  EXPECT_TRUE(alive);
  p1 = CoownedPointer<Target>();
  EXPECT_TRUE(alive);
  p2 = p1;
  EXPECT_FALSE(alive);
}

TEST(CoownedPointer, NullPointerBehavesAsReleased) {
  CoownedPointer<Target> p;
  EXPECT_FALSE(p.is_owner());
  p.release();
  EXPECT_FALSE(p.is_owner());
  p.release();  // Should be possible to call multiple times.
  EXPECT_FALSE(p.is_owner());
}

TEST(CoownedPointer, ExplicitNullPointerBehavesAsReleased) {
  CoownedPointer<Target> p(NULL);
  EXPECT_FALSE(p.is_owner());
  p.release();
  EXPECT_FALSE(p.is_owner());
  p.release();  // Should be possible to call multiple times.
  EXPECT_FALSE(p.is_owner());
}

TEST(CoownedPointer, NullPointerReassignedUnreleases) {
  CoownedPointer<Target> p1;
  EXPECT_FALSE(p1.is_owner());
  bool alive;
  CoownedPointer<Target> p2(new Target(&alive));
  EXPECT_TRUE(p2.is_owner());
  p1 = p2;
  EXPECT_TRUE(p1.is_owner());
}

TEST(CoownedPointer, ReleaseReassignedUnreleases) {
  bool alive1, alive2;
  CoownedPointer<Target> p1(new Target(&alive1));
  EXPECT_TRUE(p1.is_owner());
  delete p1.release();
  EXPECT_FALSE(p1.is_owner());
  CoownedPointer<Target> p2(new Target(&alive2));
  EXPECT_TRUE(p2.is_owner());
  p1 = p2;
  EXPECT_TRUE(p1.is_owner());
}

TEST(CoownedPointer, ReleaseWorks) {
  bool alive;
  {
    scoped_ptr<Target> released;
    {
      CoownedPointer<Target> p(new Target(&alive));
      EXPECT_TRUE(p.is_owner());
      released.reset(p.release());
      EXPECT_TRUE(alive);
      EXPECT_FALSE(p.is_owner());
    }
    EXPECT_TRUE(alive);
  }
  EXPECT_FALSE(alive);
}

TEST(CoownedPointer, ReleaseAfterMultipleCopiesWorks) {
  bool alive;
  {
    scoped_ptr<Target> released;
    {
      CoownedPointer<Target> p1(new Target(&alive));
      CoownedPointer<Target> p2(p1);
      CoownedPointer<Target> p3(p1);
      CoownedPointer<Target> p4(p2);
      EXPECT_TRUE(p1.is_owner());
      EXPECT_TRUE(p2.is_owner());
      EXPECT_TRUE(p3.is_owner());
      EXPECT_TRUE(p4.is_owner());
      released.reset(p2.release());
      EXPECT_TRUE(alive);
      EXPECT_FALSE(p1.is_owner());
      EXPECT_FALSE(p2.is_owner());
      EXPECT_FALSE(p3.is_owner());
      EXPECT_FALSE(p4.is_owner());
      {
        CoownedPointer<Target> p3(p1);
        CoownedPointer<Target> p4(p2);
        EXPECT_TRUE(alive);
        EXPECT_FALSE(p3.is_owner());
        EXPECT_FALSE(p4.is_owner());
      }
      CoownedPointer<Target> p5(p2);
      EXPECT_TRUE(alive);
      EXPECT_FALSE(p5.is_owner());
    }
    EXPECT_TRUE(alive);
  }
  EXPECT_FALSE(alive);
}

TEST(CoownedPointer, ReleaseMixedWithAssignmentWorks) {
  bool alive;
  {
    scoped_ptr<Target> released;
    {
      CoownedPointer<Target> p1(new Target(&alive));
      CoownedPointer<Target> p2;
      EXPECT_TRUE(alive);
      EXPECT_TRUE(p1.is_owner());
      EXPECT_FALSE(p2.is_owner());
      p2 = p1;
      EXPECT_TRUE(p2.is_owner());
      EXPECT_TRUE(alive);
      released.reset(p2.release());
      EXPECT_FALSE(p1.is_owner());
      EXPECT_FALSE(p2.is_owner());
      {
        CoownedPointer<Target> p3(p1);
        p3 = p2;
        EXPECT_FALSE(p3.is_owner());
        CoownedPointer<Target> p4(p2);
        p4 = p3;
        EXPECT_FALSE(p3.is_owner());
        EXPECT_TRUE(alive);
      }
      CoownedPointer<Target> p5(p2);
      CoownedPointer<Target>& p6(p5);
      EXPECT_FALSE(p5.is_owner());
      p5 = p6;  // Testing self-assignment.
      p5 = p2;
      EXPECT_TRUE(alive);
    }
    EXPECT_TRUE(alive);
  }
  EXPECT_FALSE(alive);
}

}  // namespace common
