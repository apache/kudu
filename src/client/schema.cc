// Copyright (c) 2014, Cloudera,inc.

#include "client/schema.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>

#include "common/schema.h"

namespace kudu {

namespace client {

using std::vector;

KuduColumnSchema::KuduColumnSchema(const std::string &name,
                                   DataType type,
                                   bool is_nullable,
                                   const void* default_value,
                                   KuduColumnStorageAttributes attributes) {
  ColumnStorageAttributes attr_private(attributes.encoding(),
                                       attributes.compression());
  col_.reset(new ColumnSchema(name, type, is_nullable,
                              default_value, default_value, attr_private));
}

KuduColumnSchema::KuduColumnSchema(const KuduColumnSchema& other) {
  CopyFrom(other);
}

KuduColumnSchema::~KuduColumnSchema() {}

KuduColumnSchema& KuduColumnSchema::operator=(const KuduColumnSchema& other) {
  if (&other != this) {
    CopyFrom(other);
  }
  return *this;
}

void KuduColumnSchema::CopyFrom(const KuduColumnSchema& other) {
  col_.reset(new ColumnSchema(*other.col_));
}

KuduSchema::KuduSchema() {}

KuduSchema::KuduSchema(const vector<KuduColumnSchema>& columns, int key_columns) {
  Reset(columns, key_columns);
}

KuduSchema::KuduSchema(const KuduSchema& other) {
  CopyFrom(other);
}

KuduSchema::~KuduSchema() {}

KuduSchema& KuduSchema::operator=(const KuduSchema& other) {
  if (&other != this) {
    CopyFrom(other);
  }
  return *this;
}

void KuduSchema::CopyFrom(const KuduSchema& other) {
  schema_.reset(new Schema(*other.schema_));
}

void KuduSchema::Reset(const vector<KuduColumnSchema>& columns, int key_columns) {
  vector<ColumnSchema> cols_private;
  BOOST_FOREACH(const KuduColumnSchema& col, columns) {
    cols_private.push_back(*col.col_);
  }
  schema_.reset(new Schema(cols_private, key_columns));
}

bool KuduSchema::Equals(const KuduSchema& other) const {
  return this == &other ||
      (schema_ && other.schema_ && schema_->Equals(*other.schema_));
}

KuduColumnSchema KuduSchema::Column(size_t idx) const {
  ColumnSchema col(schema_->column(idx));
  KuduColumnStorageAttributes attrs(col.attributes().encoding(),
                                    col.attributes().compression());
  return KuduColumnSchema(col.name(), col.type_info()->type(),
                          col.is_nullable(), col.read_default_value(),
                          attrs);
}

KuduSchema KuduSchema::CreateKeyProjection() const {
  KuduSchema projection;
  projection.schema_.reset(new Schema(schema_->CreateKeyProjection()));
  return projection;
}

} // namespace client
} // namespace kudu
