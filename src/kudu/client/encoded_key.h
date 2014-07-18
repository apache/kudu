// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_ENCODED_KEY_H
#define KUDU_CLIENT_ENCODED_KEY_H

#include <string>
#include <vector>

#include "kudu/client/schema.h"
#include "kudu/gutil/kudu_export.h"

namespace kudu {

class EncodedKey;
class EncodedKeyBuilder;

namespace client {

using std::string;

class KUDU_EXPORT KuduEncodedKey {
 public:
  ~KuduEncodedKey();

  std::string ToString() const;

 private:
  friend class KuduEncodedKeyBuilder;

  explicit KuduEncodedKey(EncodedKey* key);

  gscoped_ptr<EncodedKey> key_;
};

class KUDU_EXPORT KuduEncodedKeyBuilder {
 public:
  explicit KuduEncodedKeyBuilder(const KuduSchema& schema);
  ~KuduEncodedKeyBuilder();

  void Reset();

  void AddColumnKey(const void* raw_key);

  KuduEncodedKey* BuildEncodedKey();

 private:
  gscoped_ptr<EncodedKeyBuilder> key_builder_;
};

} // namespace client
} // namespace kudu
#endif // KUDU_CLIENT_ENCODED_KEY_H_
