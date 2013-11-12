// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_JSONWRITER_H
#define KUDU_UTIL_JSONWRITER_H

#include <inttypes.h>

#include <string>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"

namespace kudu {

class JsonWriterImpl;

// Acts as a pimpl for rapidjson so that not all metrics users must bring in the
// rapidjson library, which is template-based and therefore hard to forward-declare.
//
// This class implements all the methods of rapidjson::JsonWriter, plus an
// additional convenience method for String(std::string).
// TODO: Add Protobuf(Message* proto) output to JSON.
//
// We take an instance of std::stringstream in the constructor because Mongoose / Squeasel
// uses std::stringstream for output buffering.
class JsonWriter {
 public:
  explicit JsonWriter(std::stringstream* out);
  ~JsonWriter();

  void Null();
  void Bool(bool b);
  void Int(int i);
  void Uint(unsigned u);
  void Int64(int64_t i64);
  void Uint64(uint64_t u64);
  void Double(double d);
  void String(const char* str, size_t length);
  void String(const char* str);
  void String(const std::string& str);

  template<typename T>
  void Value(const T& val);

  void StartObject();
  void EndObject();
  void StartArray();
  void EndArray();

 private:
  gscoped_ptr<JsonWriterImpl> impl_;
  DISALLOW_COPY_AND_ASSIGN(JsonWriter);
};

} // namespace kudu

#endif // KUDU_UTIL_JSONWRITER_H
