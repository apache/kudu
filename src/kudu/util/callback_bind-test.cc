// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/gutil/bind.h"
#include "kudu/gutil/callback.h"

#include <gtest/gtest.h>

namespace kudu {

using std::string;

static int Return5() {
  return 5;
}

TEST(CallbackBindTest, TestFreeFunction) {
  Callback<int(void)> func_cb = Bind(&Return5);
  ASSERT_EQ(5, func_cb.Run());
}

class Ref : public RefCountedThreadSafe<Ref> {
 public:
  int Foo() { return 3; }
};

TEST(CallbackBindTest, TestClassMethod) {
  scoped_refptr<Ref> ref = new Ref();
  Callback<int(void)> ref_cb = Bind(&Ref::Foo, ref);
  ref = NULL;
  ASSERT_EQ(3, ref_cb.Run());
}

int ReturnI(int i, const char* str) {
  return i;
}

TEST(CallbackBindTest, TestPartialBind) {
  Callback<int(const char*)> cb = Bind(&ReturnI, 23);
  ASSERT_EQ(23, cb.Run("hello world"));
}

char IncrementChar(gscoped_ptr<char> in) {
  return *in + 1;
}

TEST(CallbackBindTest, TestCallScopedPtrArg) {
  // Calling a function with a gscoped_ptr argument is just like any other
  // function which takes gscoped_ptr:
  gscoped_ptr<char> foo(new char('x'));
  Callback<char(gscoped_ptr<char>)> cb = Bind(&IncrementChar);
  ASSERT_EQ('y', cb.Run(foo.Pass()));
}

TEST(CallbackBindTest, TestBindScopedPtrArg) {
  // Binding a function with a gscoped_ptr argument requires using Passed()
  gscoped_ptr<char> foo(new char('x'));
  Callback<char(void)> cb = Bind(&IncrementChar, Passed(&foo));
  ASSERT_EQ('y', cb.Run());
}

} // namespace kudu
