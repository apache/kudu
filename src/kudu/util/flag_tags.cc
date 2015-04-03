// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/flag_tags.h"

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/singleton.h"

#include <map>
#include <tr1/unordered_set>
#include <string>
#include <utility>
#include <vector>

using std::multimap;
using std::pair;
using std::string;
using std::tr1::unordered_set;
using std::vector;

namespace kudu {
namespace flag_tags_internal {

// Singleton registry storing the set of tags for each flag.
class FlagTagRegistry {
 public:
  static FlagTagRegistry* GetInstance() {
    return Singleton<FlagTagRegistry>::get();
  }

  void Tag(const string& name, const string& tag) {
    tag_map_.insert(TagMap::value_type(name, tag));
  }

  void GetTags(const string& name, unordered_set<string>* tags) {
    tags->clear();
    pair<TagMap::const_iterator, TagMap::const_iterator> range =
      tag_map_.equal_range(name);
    for (TagMap::const_iterator it = range.first;
         it != range.second;
         ++it) {
      if (!InsertIfNotPresent(tags, it->second)) {
        LOG(DFATAL) << "Flag " << name << " was tagged more than once with the tag '"
                    << it->second << "'";
      }
    }
  }

 private:
  friend class Singleton<FlagTagRegistry>;
  FlagTagRegistry() {}

  typedef multimap<string, string> TagMap;
  TagMap tag_map_;

  DISALLOW_COPY_AND_ASSIGN(FlagTagRegistry);
};


FlagTagger::FlagTagger(const char* name, const char* tag) {
  FlagTagRegistry::GetInstance()->Tag(name, tag);
}

FlagTagger::~FlagTagger() {
}

} // namespace flag_tags_internal

using flag_tags_internal::FlagTagRegistry;

void GetFlagTags(const string& flag_name,
                 unordered_set<string>* tags) {
  FlagTagRegistry::GetInstance()->GetTags(flag_name, tags);
}

} // namespace kudu
