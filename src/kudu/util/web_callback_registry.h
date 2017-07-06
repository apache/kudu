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
#ifndef KUDU_UTIL_WEB_CALLBACK_REGISTRY_H
#define KUDU_UTIL_WEB_CALLBACK_REGISTRY_H

#include <iosfwd>
#include <map>
#include <string>

#include <boost/function.hpp>

#include "kudu/util/easy_json.h"

namespace kudu {

// Interface for registering webserver callbacks.
//
// To register a webserver callback for /example/path:
//
// 1. Define a PathHandlerCallback that accepts an EasyJson
//    object and fills out its fields with relevant information.
// 2. Call RegisterPathHandler("/example/path", ...)
// 3. Create the file $KUDU_HOME/www/example/path.mustache
class WebCallbackRegistry {
 public:
  typedef std::map<std::string, std::string> ArgumentMap;

  struct WebRequest {
    // The query string, parsed into key/value argument pairs.
    ArgumentMap parsed_args;

    // The raw query string passed in the URL. May be empty.
    std::string query_string;

    // The method (POST/GET/etc).
    std::string request_method;

    // In the case of a POST, the posted data.
    std::string post_data;
  };

  // A function that adds members to the JSON object 'output' to be
  // rendered in an HTML template.
  typedef boost::function<void (const WebRequest& args, EasyJson* output)>
      PathHandlerCallback;

  // A function that streams fully rendered HTML to 'output'.
  typedef boost::function<void (const WebRequest& args, std::ostringstream* output)>
      PrerenderedPathHandlerCallback;

  virtual ~WebCallbackRegistry() {}

  // Register a callback for a URL path. Path should not include the
  // http://hostname/ prefix. If is_styled is true, the page is meant to be for
  // people to look at and is styled.  If false, it is meant to be for machines to
  // scrape.  If is_on_nav_bar is true,  a link to this page is
  // printed in the navigation bar at the top of each debug page. Otherwise the
  // link does not appear, and the page is rendered without HTML headers and
  // footers.
  // The first registration's choice of is_styled overrides all
  // subsequent registrations for that URL.
  // For each call to RegisterPathHandler(), the file $KUDU_HOME/www<path>.mustache
  // should exist.
  virtual void RegisterPathHandler(const std::string& path, const std::string& alias,
                                   const PathHandlerCallback& callback,
                                   bool is_styled, bool is_on_nav_bar) = 0;

  // Same as RegisterPathHandler(), except that callback produces prerendered HTML.
  // Use RegisterPathHandler() with a mustache template instead.
  virtual void RegisterPrerenderedPathHandler(const std::string& path, const std::string& alias,
                                              const PrerenderedPathHandlerCallback& callback,
                                              bool is_styled,
                                              bool is_on_nav_bar) = 0;
};

} // namespace kudu

#endif /* KUDU_UTIL_WEB_CALLBACK_REGISTRY_H */
