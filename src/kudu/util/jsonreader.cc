// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/jsonreader.h"

#include "kudu/gutil/strings/substitute.h"

using rapidjson::Value;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

JsonReader::JsonReader(const string& text)
  : text_(text) {
}

JsonReader::~JsonReader() {
}

Status JsonReader::Init() {
  document_.Parse<0>(text_.c_str());
  if (document_.HasParseError()) {
    return Status::Corruption("JSON text is corrupt", document_.GetParseError());
  }
  return Status::OK();
}

Status JsonReader::ExtractInt32(const Value* object,
                                const char* field,
                                int32_t* result) const {
  const Value* val;
  RETURN_NOT_OK(ExtractField(object, field, &val));
  if (PREDICT_FALSE(!val->IsInt())) {
    return Status::InvalidArgument(Substitute(
        "Wrong type during field extraction: expected int32 but got $0",
        val->GetType()));
  }
  *result = val->GetUint();
  return Status::OK();
}

Status JsonReader::ExtractInt64(const Value* object,
                                const char* field,
                                int64_t* result) const {
  const Value* val;
  RETURN_NOT_OK(ExtractField(object, field, &val));
  if (PREDICT_FALSE(!val->IsInt64())) {
    return Status::InvalidArgument(Substitute(
        "Wrong type during field extraction: expected int64 but got $0",
        val->GetType()));  }
  *result = val->GetUint64();
  return Status::OK();
}

Status JsonReader::ExtractString(const Value* object,
                                 const char* field,
                                 string* result) const {
  const Value* val;
  RETURN_NOT_OK(ExtractField(object, field, &val));
  if (PREDICT_FALSE(!val->IsString())) {
    if (val->IsNull()) {
      *result = "";
      return Status::OK();
    }
    return Status::InvalidArgument(Substitute(
        "Wrong type during field extraction: expected string but got $0",
        val->GetType()));  }
  result->assign(val->GetString());
  return Status::OK();
}

Status JsonReader::ExtractObject(const Value* object,
                                 const char* field,
                                 const Value** result) const {
  const Value* val;
  RETURN_NOT_OK(ExtractField(object, field, &val));
  if (PREDICT_FALSE(!val->IsObject())) {
    return Status::InvalidArgument(Substitute(
        "Wrong type during field extraction: expected object but got $0",
        val->GetType()));  }
  *result = val;
  return Status::OK();
}

Status JsonReader::ExtractObjectArray(const Value* object,
                                      const char* field,
                                      vector<const Value*>* result) const {
  const Value* val;
  RETURN_NOT_OK(ExtractField(object, field, &val));
  if (PREDICT_FALSE(!val->IsArray())) {
    return Status::InvalidArgument(Substitute(
        "Wrong type during field extraction: expected object array but got $0",
        val->GetType()));  }
  for (Value::ConstValueIterator iter = val->Begin(); iter != val->End(); ++iter) {
    result->push_back(iter);
  }
  return Status::OK();
}

Status JsonReader::ExtractField(const Value* object,
                                const char* field,
                                const Value** result) const {
  if (field && PREDICT_FALSE(!object->HasMember(field))) {
    return Status::NotFound("Missing field", field);
  }
  *result = field ? &(*object)[field] : object;
  return Status::OK();
}

} // namespace kudu
