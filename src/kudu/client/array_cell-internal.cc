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


#include <cstdint>
#include <cstring>

#include "kudu/client/array_cell.h"
#include "kudu/client/schema-internal.h"
#include "kudu/client/schema.h"
#include "kudu/common/array_cell_view.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

class KuduArrayCellView::Data {
 public:
  Data(const uint8_t* buf, const size_t size)
      : view_(buf, size) {
  }

  ~Data() = default;

  ArrayCellMetadataView view_;
};

KuduArrayCellView::KuduArrayCellView(const void* cell_ptr) {
  if (!cell_ptr) {
    data_ = new Data(nullptr, 0);
  } else {
    const Slice* cell = reinterpret_cast<const Slice*>(cell_ptr);
    data_ = new Data(cell->data(), cell->size());
  }
}

KuduArrayCellView::KuduArrayCellView(const uint8_t* buf, const size_t size)
    : data_(new Data(buf, size)) {
}

KuduArrayCellView::~KuduArrayCellView() {
  delete data_;
}

Status KuduArrayCellView::Init() {
  return data_->view_.Init();
}

size_t KuduArrayCellView::elem_num() const {
  return data_->view_.elem_num();
}

bool KuduArrayCellView::empty() const {
  return data_->view_.empty();
}

bool KuduArrayCellView::has_nulls() const {
  return data_->view_.has_nulls();
}

const uint8_t* KuduArrayCellView::not_null_bitmap() const {
  return data_->view_.not_null_bitmap();
}

const void* KuduArrayCellView::data(KuduColumnSchema::DataType data_type,
                                    const KuduColumnTypeAttributes& attributes) const {
  return data_->view_.data_as(ToInternalDataType(data_type, attributes));
}

} // namespace client
} // namespace kudu
