// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/util/yamlreader.h"

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <yaml-cpp/node/impl.h>
#include <yaml-cpp/node/node.h>

#include "kudu/util/env.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

class YamlReaderTest : public KuduTest {
public:
  Status GenerateYamlReader(const string& content, unique_ptr<YamlReader>* result) {
    string fname = GetTestPath("YamlReaderTest.json");
    unique_ptr<WritableFile> writable_file;
    RETURN_NOT_OK(env_->NewWritableFile(fname, &writable_file));
    RETURN_NOT_OK(writable_file->Append(Slice(content)));
    RETURN_NOT_OK(writable_file->Close());
    result->reset(new YamlReader(fname));
    return Status::OK();
  }
};

TEST_F(YamlReaderTest, FileNotExist) {
  YamlReader r("YamlReaderTest.NotExist");
  Status s = r.Init();
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_STR_CONTAINS(
      s.ToString(), "YAML::LoadFile error");
}

TEST_F(YamlReaderTest, Corruption) {
  unique_ptr<YamlReader> r;
  ASSERT_OK(GenerateYamlReader("foo", &r));
  ASSERT_OK(r->Init());
  int val = 0;
  ASSERT_TRUE(YamlReader::ExtractScalar(r->node(), "foo", &val).IsCorruption());
}

TEST_F(YamlReaderTest, EmptyFile) {
  unique_ptr<YamlReader> r;
  ASSERT_OK(GenerateYamlReader("", &r));
  ASSERT_OK(r->Init());

  int val = 0;
  ASSERT_TRUE(YamlReader::ExtractScalar(r->node(), "foo", &val).IsCorruption());
}

TEST_F(YamlReaderTest, KeyNotExist) {
  unique_ptr<YamlReader> r;
  ASSERT_OK(GenerateYamlReader("foo: 1", &r));
  ASSERT_OK(r->Init());

  int val = 0;
  ASSERT_TRUE(YamlReader::ExtractScalar(r->node(), "bar", &val).IsNotFound());
}

TEST_F(YamlReaderTest, Scalar) {
  {
    unique_ptr<YamlReader> r;
    ASSERT_OK(GenerateYamlReader("bool_val: false", &r));
    ASSERT_OK(r->Init());
    bool val = true;
    ASSERT_OK(YamlReader::ExtractScalar(r->node(), "bool_val", &val));
    ASSERT_EQ(val, false);
  }

  {
    unique_ptr<YamlReader> r;
    ASSERT_OK(GenerateYamlReader("int_val: 123", &r));
    ASSERT_OK(r->Init());
    int val = 0;
    ASSERT_OK(YamlReader::ExtractScalar(r->node(), "int_val", &val));
    ASSERT_EQ(val, 123);
  }

  {
    unique_ptr<YamlReader> r;
    ASSERT_OK(GenerateYamlReader("double_val: 123.456", &r));
    ASSERT_OK(r->Init());
    double val = 0.0;
    ASSERT_OK(YamlReader::ExtractScalar(r->node(), "double_val", &val));
    ASSERT_EQ(val, 123.456);
  }

  {
    unique_ptr<YamlReader> r;
    ASSERT_OK(GenerateYamlReader("string_val: hello yaml", &r));
    ASSERT_OK(r->Init());
    string val;
    ASSERT_OK(YamlReader::ExtractScalar(r->node(), "string_val", &val));
    ASSERT_EQ(val, "hello yaml");
  }
}

TEST_F(YamlReaderTest, Map) {
  unique_ptr<YamlReader> r;
  ASSERT_OK(GenerateYamlReader(
      "map_val: { key1: hello yaml, key2: 123.456 , key3: 123, key4: false}", &r));
  ASSERT_OK(r->Init());

  YAML::Node node;
  ASSERT_OK(YamlReader::ExtractMap(r->node(), "map_val", &node));

  {
    string val;
    ASSERT_OK(YamlReader::ExtractScalar(&node, "key1", &val));
    ASSERT_EQ(val, "hello yaml");
  }

  {
    double val = 0.0;
    ASSERT_OK(YamlReader::ExtractScalar(&node, "key2", &val));
    ASSERT_EQ(val, 123.456);
  }

  {
    int val = 0;
    ASSERT_OK(YamlReader::ExtractScalar(&node, "key3", &val));
    ASSERT_EQ(val, 123);
  }

  {
    bool val = true;
    ASSERT_OK(YamlReader::ExtractScalar(&node, "key4", &val));
    ASSERT_EQ(val, false);
  }

  // Not exist key.
  {
    int val = 0;
    ASSERT_TRUE(YamlReader::ExtractScalar(&node, "key5", &val).IsNotFound());
  }
}

TEST_F(YamlReaderTest, Array) {
  unique_ptr<YamlReader> r;
  ASSERT_OK(GenerateYamlReader("list_val: [1, 3, 5, 7, 9]", &r));
  ASSERT_OK(r->Init());
  const std::vector<int>& expect_vals = { 1, 3, 5, 7, 9 };

  std::vector<int> vals;
  ASSERT_OK(YamlReader::ExtractArray(r->node(), "list_val", &vals));
  ASSERT_EQ(vals, expect_vals);
}

} // namespace kudu
