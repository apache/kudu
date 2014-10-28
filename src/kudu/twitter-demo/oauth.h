// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TWITTERDEMO_OAUTH_H
#define KUDU_TWITTERDEMO_OAUTH_H

#include <gtest/gtest_prod.h>

#include <string>
#include <utility>
#include <vector>

namespace kudu {
namespace twitter_demo {

// An OpenAuth-authenticated request. See oauth-test.cc for
// usage examples.
class OAuthRequest {
 private:
  typedef std::pair<std::string, std::string> StringPair;

 public:
  OAuthRequest(const std::string& http_method,
               const std::string& url);

  // Add a key-value pair to the OAauth request.
  void AddPair(const std::string& key, const std::string& value);

  // Add the standard OAuth fields to the request, including
  // generating a nonce and filling in the request timestamp.
  void AddStandardOAuthFields(const std::string& consumer_key,
                              const std::string& token_key);

  // Generate the HTTP Authorization header to authenticate this request.
  // This is the entire header, including the 'Authorization: ' prefix.
  std::string AuthHeader(const std::string& consumer_secret,
                         const std::string& token_secret) const;

 private:
  FRIEND_TEST(OAuthTest, TestSignature);

  std::string SignatureBaseString() const;
  std::string Signature(const std::string& consumer_secret,
                        const std::string& token_secret) const;

  std::string http_method_;
  std::string url_;

  // The entries used in the request.
  std::vector<StringPair > kv_pairs_;
};

} // namespace twitter_demo
} // namespace kudu
#endif
