// Copyright (c) 2014, Cloudera,inc.

#include "kudu/client/encoded_key.h"

#include "kudu/common/encoded_key.h"

namespace kudu {

namespace client {

using std::string;

KuduEncodedKey::~KuduEncodedKey() {}

KuduEncodedKey::KuduEncodedKey(EncodedKey* key)
  : key_(key) {}

string KuduEncodedKey::ToString() const {
  return key_->encoded_key().ToString();
}

KuduEncodedKeyBuilder::KuduEncodedKeyBuilder(const KuduSchema& schema)
  : key_builder_(new EncodedKeyBuilder(schema.schema_.get())) {
}

KuduEncodedKeyBuilder::~KuduEncodedKeyBuilder() {}

void KuduEncodedKeyBuilder::Reset() {
  key_builder_->Reset();
}

void KuduEncodedKeyBuilder::AddColumnKey(const void* raw_key) {
  key_builder_->AddColumnKey(raw_key);
}

KuduEncodedKey* KuduEncodedKeyBuilder::BuildEncodedKey() {
  return new KuduEncodedKey(key_builder_->BuildEncodedKey());
}

} // namespace client
} // namespace kudu
