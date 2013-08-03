// Copyright (c) 2013, Cloudera,inc.

#include <gtest/gtest.h>

#include <tr1/memory>

#include "gutil/gscoped_ptr.h"
#include "util/env_util.h"
#include "util/memenv/memenv.h"
#include "util/pb_util.h"
#include "util/pb_util-internal.h"
#include "util/status.h"
#include "util/test_util.h"

using std::tr1::shared_ptr;

namespace kudu {

class TestPBUtil : public KuduTest {};

TEST_F(TestPBUtil, TestWritableFileOutputStream) {
  gscoped_ptr<Env> env(NewMemEnv(Env::Default()));
  shared_ptr<WritableFile> file;
  ASSERT_STATUS_OK(env_util::OpenFileForWrite(env.get(), "/test", &file));

  WritableFileOutputStream stream(file.get(), 4096);

  void* buf;
  int size;

  // First call should yield the whole buffer.
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(4096, stream.ByteCount());

  // Backup 1000 and the next call should yield 1000
  stream.BackUp(1000);
  ASSERT_EQ(3096, stream.ByteCount());

  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(1000, size);

  // Another call should flush and yield a new buffer of 4096
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(8192, stream.ByteCount());

  // Should be able to backup to 7192
  stream.BackUp(1000);
  ASSERT_EQ(7192, stream.ByteCount());

  // Flushing shouldn't change written count.
  ASSERT_TRUE(stream.Flush());
  ASSERT_EQ(7192, stream.ByteCount());

  // Since we just flushed, we should get another full buffer.
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(7192 + 4096, stream.ByteCount());

  ASSERT_TRUE(stream.Flush());

  ASSERT_EQ(stream.ByteCount(), file->Size());
}

} // namespace kudu
