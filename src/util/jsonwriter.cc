// Copyright (c) 2013, Cloudera, inc.
#include "util/jsonwriter.h"

#include <string>

#include <glog/logging.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>

using std::string;
using std::stringstream;

namespace kudu {

// Adapter to allow RapidJSON to write directly to a stringstream.
// Since Squeasel exposes a stringstream as its interface, this is needed to avoid overcopying.
class UTF8StringStreamBuffer {
 public:
  explicit UTF8StringStreamBuffer(std::stringstream* out);
  void Put(rapidjson::UTF8<>::Ch c);
 private:
  std::stringstream* out_;
};

class JsonWriterImpl {
 public:
  explicit JsonWriterImpl(stringstream* out);

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

  void StartObject();
  void EndObject();
  void StartArray();
  void EndArray();

 private:
  UTF8StringStreamBuffer stream_;
  rapidjson::PrettyWriter<UTF8StringStreamBuffer> writer_;
  DISALLOW_COPY_AND_ASSIGN(JsonWriterImpl);
};

//
// JsonWriter
//

JsonWriter::JsonWriter(stringstream* out)
  : impl_(new JsonWriterImpl(DCHECK_NOTNULL(out))) {
}
JsonWriter::~JsonWriter() {
}
void JsonWriter::Null() { impl_->Null(); }
void JsonWriter::Bool(bool b) { impl_->Bool(b); }
void JsonWriter::Int(int i) { impl_->Int(i); }
void JsonWriter::Uint(unsigned u) { impl_->Uint(u); }
void JsonWriter::Int64(int64_t i64) { impl_->Int64(i64); }
void JsonWriter::Uint64(uint64_t u64) { impl_->Uint64(u64); }
void JsonWriter::Double(double d) { impl_->Double(d); }
void JsonWriter::String(const char* str, size_t length) { impl_->String(str, length); }
void JsonWriter::String(const char* str) { impl_->String(str); }
void JsonWriter::String(const string& str) { impl_->String(str); }
void JsonWriter::StartObject() { impl_->StartObject(); }
void JsonWriter::EndObject() { impl_->EndObject(); }
void JsonWriter::StartArray() { impl_->StartArray(); }
void JsonWriter::EndArray() { impl_->EndArray(); }

//
// UTF8StringStreamBuffer
//

UTF8StringStreamBuffer::UTF8StringStreamBuffer(std::stringstream* out)
  : out_(DCHECK_NOTNULL(out)) {
}

void UTF8StringStreamBuffer::Put(rapidjson::UTF8<>::Ch c) {
  out_->put(c);
}

//
// JsonWriterImpl
//

JsonWriterImpl::JsonWriterImpl(stringstream* out)
  : stream_(DCHECK_NOTNULL(out)),
    writer_(stream_) {
}
void JsonWriterImpl::Null() { writer_.Null(); }
void JsonWriterImpl::Bool(bool b) { writer_.Bool(b); }
void JsonWriterImpl::Int(int i) { writer_.Int(i); }
void JsonWriterImpl::Uint(unsigned u) { writer_.Uint(u); }
void JsonWriterImpl::Int64(int64_t i64) { writer_.Int64(i64); }
void JsonWriterImpl::Uint64(uint64_t u64) { writer_.Uint64(u64); }
void JsonWriterImpl::Double(double d) { writer_.Double(d); }
void JsonWriterImpl::String(const char* str, size_t length) { writer_.String(str, length); }
void JsonWriterImpl::String(const char* str) { writer_.String(str); }
void JsonWriterImpl::String(const string& str) { writer_.String(str.c_str(), str.length()); }
void JsonWriterImpl::StartObject() { writer_.StartObject(); }
void JsonWriterImpl::EndObject() { writer_.EndObject(); }
void JsonWriterImpl::StartArray() { writer_.StartArray(); }
void JsonWriterImpl::EndArray() { writer_.EndArray(); }

} // namespace kudu
