// Copyright (c) 2014, Cloudera, inc.

#include "gutil/bind.h"
#include "gutil/callback.h"

#include <gtest/gtest.h>

namespace kudu {

using base::Bind;
using base::Callback;
using std::string;

static int Return5() {
  return 5;
}

TEST(CallbackBindTest, TestFreeFunction) {
  Callback<int(void)> func_cb = Bind(&Return5);
  ASSERT_EQ(5, func_cb.Run());
}

class Ref : public base::RefCountedThreadSafe<Ref> {
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

} // namespace kudu
