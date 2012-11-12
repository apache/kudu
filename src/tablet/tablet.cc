// Copyright (c) 2012, Cloudera, inc.


#include <boost/foreach.hpp>
#include <tr1/memory>
#include <vector>

#include "cfile/cfile.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "tablet/tablet.h"
#include "tablet/layer.h"
#include "util/env.h"

namespace kudu { namespace tablet {

using std::string;
using std::vector;
using std::tr1::shared_ptr;


const string kLayerPrefix = "layer_";

Tablet::Tablet(const Schema &schema,
               const string &dir) :
  schema_(schema),
  dir_(dir),
  memstore_(new MemStore(schema)),
  env_(Env::Default()),
  open_(false)
{
}

Status Tablet::CreateNew() {
  CHECK(!open_) << "already open";
  RETURN_NOT_OK(env_->CreateDir(dir_));
  // TODO: write a metadata file into the tablet dir
  return Status::OK();
}

Status Tablet::Open() {
  CHECK(!open_) << "already open";
  // TODO: read metadata file, open layer readers for flushed files.
  // TODO: track a state_ variable, ensure tablet is open, etc.

  // for now, just list the children, to make sure the dir exists.
  vector<string> children;
  RETURN_NOT_OK(env_->GetChildren(dir_, &children));

  BOOST_FOREACH(const string &child, children) {
    string suffix;
    if (TryStripPrefixString(child, kLayerPrefix, &suffix)) {
      uint32_t layer_idx;
      if (!safe_strtou32(child.c_str(), &layer_idx)) {
        return Status::IOError(string("Bad layer file: ") + child);
      }
    } else {
      LOG(WARNING) << "ignoring unknown file in " << dir_  << ": " << child;
    }
  }

  open_ = true;

  return Status::OK();
}

Status Tablet::Insert(const Slice &data) {
  CHECK(open_) << "must Open() first!";

  return memstore_->Insert(data);
}

Status Tablet::Flush() {
  // swap in a new memstore
  scoped_ptr<MemStore> old_ms(new MemStore(schema_));
  old_ms.swap(memstore_);

  // TODO: will need to think carefully about handling concurrent
  // updates during the flush process. For initial prototype, ignore
  // this tricky bit.
  LayerWriter out(env_, schema_, dir_ + "/flush.tmp");
  RETURN_NOT_OK(out.Open());

  scoped_ptr<MemStore::Iterator> iter(old_ms->NewIterator());
  CHECK(iter->IsValid()) << "old memstore yielded invalid iterator";

  int written = 0;
  while (iter->IsValid()) {
    Slice s = iter->GetCurrentRow();
    Status status = out.WriteRow(s);
    if (!status.ok()) {
      LOG(ERROR) << "Unable to write row " << written << " to " <<
        dir_ << ": " << status.ToString();
      return status;
    }
    iter->Next();
    written++;
  }

  return out.Finish();
}


} // namespace table
} // namespace kudu
