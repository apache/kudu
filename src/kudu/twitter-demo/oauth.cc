// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/twitter-demo/oauth.h"

#include <algorithm>
#include <vector>
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>
#include <glog/logging.h>
extern "C" {
#include <oauth.h>
}

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/util.h"

using std::pair;
using std::string;
using std::vector;

namespace kudu {
namespace twitter_demo {

static string EscapeUrl(const string& str) {
  gscoped_ptr<char, FreeDeleter> enc(oauth_url_escape(str.c_str()));
  return string(enc.get());
}

static string GenerateNonce() {
  gscoped_ptr<char, FreeDeleter> ret(oauth_gen_nonce());
  return string(ret.get());
}


OAuthRequest::OAuthRequest(const string& http_method,
                           const string& url)
  : http_method_(http_method),
    url_(url) {
}

void OAuthRequest::AddStandardOAuthFields(const string& consumer_key,
                                          const string& token_key) {
  AddPair("oauth_version", "1.0");
  AddPair("oauth_signature_method", "HMAC-SHA1");
  AddPair("oauth_nonce", GenerateNonce());
  AddPair("oauth_timestamp", boost::lexical_cast<string>(time(NULL)));
  AddPair("oauth_consumer_key", consumer_key);
  AddPair("oauth_token", token_key);
}

void OAuthRequest::AddPair(const string& key, const string& value) {
  kv_pairs_.push_back(std::make_pair(key, value));
}

static bool ComparePair(const pair<std::string, std::string>& a,
                        const pair<std::string, std::string>& b) {
  if (a.first < b.first) return true;
  else if (a.first > b.first) return false;

  return a.second < b.second;
}

string OAuthRequest::SignatureBaseString() const {
  vector<pair<string, string> > sorted_pairs(kv_pairs_);
  std::sort(sorted_pairs.begin(), sorted_pairs.end(), &ComparePair);
  string ret;
  ret.append(http_method_);
  ret.append("&");
  ret.append(EscapeUrl(url_));

  string kvpairs;
  bool first = true;
  BOOST_FOREACH(const StringPair& p, sorted_pairs) {
    if (!first) {
      kvpairs.append("&");
    }
    first = false;
    kvpairs.append(p.first);
    kvpairs.append("=");
    kvpairs.append(EscapeUrl(p.second));
  }
  ret.append("&");
  ret.append(EscapeUrl(kvpairs));
  return ret;
}

string OAuthRequest::Signature(const string& consumer_secret,
                               const string& token_secret) const {
  string base = SignatureBaseString();
  string key = consumer_secret + "&" + token_secret;
  gscoped_ptr<char, FreeDeleter> hmacced(
    oauth_sign_hmac_sha1_raw(base.c_str(), base.size(), key.c_str(), key.size()));
  CHECK(hmacced.get());
  return string(hmacced.get());
}

string OAuthRequest::AuthHeader(const string& consumer_secret,
                                const string& token_secret) const {
  string sig = Signature(consumer_secret, token_secret);

  string ret = "Authorization: OAuth realm=\"\"";
  BOOST_FOREACH(const StringPair& p, kv_pairs_) {
    if (!HasPrefixString(p.first, "oauth_")) continue;
    ret.append(", ");
    ret.append(p.first).append("=\"").append(EscapeUrl(p.second)).append("\"");
  }
  ret.append(", oauth_signature_method=\"HMAC-SHA1\"");
  ret.append(", oauth_signature=\"").append(EscapeUrl(sig)).append("\"");
  return ret;
}

} // namespace twitter_demo
} // namespace kudu
