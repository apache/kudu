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
#ifndef KUDU_UTIL_WEBSERVER_H
#define KUDU_UTIL_WEBSERVER_H

#include <iosfwd>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <squeasel.h>

#include "kudu/gutil/port.h"
#include "kudu/server/webserver_options.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/web_callback_registry.h"

namespace kudu {

class EasyJson;

// Wrapper class for the Mongoose web server library. Clients may register callback
// methods which produce output for a given URL path
class Webserver : public WebCallbackRegistry {
 public:
  // Using this constructor, the webserver will bind to all available
  // interfaces.
  explicit Webserver(const WebserverOptions& opts);

  ~Webserver();

  // Starts a webserver on the port passed to the constructor. The webserver runs in a
  // separate thread, so this call is non-blocking.
  Status Start() WARN_UNUSED_RESULT;

  // Stops the webserver synchronously.
  void Stop();

  // Return the addresses that this server has successfully
  // bound to. Requires that the server has been Start()ed.
  Status GetBoundAddresses(std::vector<Sockaddr>* addrs) const WARN_UNUSED_RESULT;

  // Return the addresses that this server is advertising externally
  // to the world. Requires that the server has been Start()ed.
  Status GetAdvertisedAddresses(std::vector<Sockaddr>* addresses) const WARN_UNUSED_RESULT;

  // Register a route 'path' to be rendered via template.
  // The appropriate template to use is determined by 'path'.
  // If 'is_styled' is true, the page will be styled and include a header and footer.
  // If 'is_on_nav_bar' is true, a link to the page will be placed on the navbar
  // in the header of styled pages. The link text is given by 'alias'.
  void RegisterPathHandler(const std::string& path, const std::string& alias,
                           const PathHandlerCallback& callback,
                           bool is_styled, bool is_on_nav_bar) override;

  // Register a route 'path'. See the RegisterPathHandler for details.
  void RegisterPrerenderedPathHandler(const std::string& path, const std::string& alias,
                                      const PrerenderedPathHandlerCallback& callback,
                                      bool is_styled,
                                      bool is_on_nav_bar) override;

  // Change the footer HTML to be displayed at the bottom of all styled web pages.
  void set_footer_html(const std::string& html);

  // True if serving all traffic over SSL, false otherwise
  bool IsSecure() const;

 private:
  // Container class for a list of path handler callbacks for a single URL.
  class PathHandler {
   public:
    PathHandler(bool is_styled, bool is_on_nav_bar, std::string alias,
                PrerenderedPathHandlerCallback callback)
        : is_styled_(is_styled),
          is_on_nav_bar_(is_on_nav_bar),
          alias_(std::move(alias)),
          callback_(std::move(callback)) {}

    bool is_styled() const { return is_styled_; }
    bool is_on_nav_bar() const { return is_on_nav_bar_; }
    const std::string& alias() const { return alias_; }
    const PrerenderedPathHandlerCallback& callback() const { return callback_; }

   private:
    // If true, the page appears is rendered styled.
    bool is_styled_;

    // If true, the page appears in the navigation bar.
    bool is_on_nav_bar_;

    // Alias used when displaying this link on the nav bar.
    std::string alias_;

    // Callback to render output for this page.
    PrerenderedPathHandlerCallback callback_;
  };

  bool static_pages_available() const;

  // Build the string to pass to mongoose specifying where to bind.
  Status BuildListenSpec(std::string* spec) const WARN_UNUSED_RESULT;

  // Returns a mustache tag that renders the partial at path when
  // passed to mustache::RenderTemplate.
  std::string MustachePartialTag(const std::string& path) const;

  // Returns whether or not a mustache template corresponding
  // to the given path can be found.
  bool MustacheTemplateAvailable(const std::string& path) const;

  // Renders the main HTML template with the pre-rendered string 'content'
  // in the main body of the page, into 'output'.
  void RenderMainTemplate(const std::string& content, std::stringstream* output);

  // Renders the template corresponding to 'path' (if available), using
  // fields in 'ej'.
  void Render(const std::string& path, const EasyJson& ej, bool use_style,
              std::stringstream* output);

  // Dispatch point for all incoming requests.
  // Static so that it can act as a function pointer, and then call the next method
  static sq_callback_result_t BeginRequestCallbackStatic(struct sq_connection* connection);
  sq_callback_result_t BeginRequestCallback(
      struct sq_connection* connection,
      struct sq_request_info* request_info);

  sq_callback_result_t RunPathHandler(
      const PathHandler& handler,
      struct sq_connection* connection,
      struct sq_request_info* request_info);

  // Callback to funnel mongoose logs through glog.
  static int LogMessageCallbackStatic(const struct sq_connection* connection,
                                      const char* message);

  // Registered to handle "/", and prints a list of available URIs
  void RootHandler(const WebRequest& args, WebResponse* resp);

  // Builds a map of argument name to argument value from a typical URL argument
  // string (that is, "key1=value1&key2=value2.."). If no value is given for a
  // key, it is entered into the map as (key, "").
  void BuildArgumentMap(const std::string& args, ArgumentMap* output);

  const WebserverOptions opts_;

  // Lock guarding the path_handlers_ map and footer_html.
  RWMutex lock_;

  // Map of path to a PathHandler containing a list of handlers for that
  // path. More than one handler may register itself with a path so that many
  // components may contribute to a single page.
  typedef std::map<std::string, PathHandler*> PathHandlerMap;
  PathHandlerMap path_handlers_;

  // Snippet of HTML which will be displayed in the footer of all pages
  // rendered by this server. Protected by 'lock_'.
  std::string footer_html_;

  // The address of the interface on which to run this webserver.
  std::string http_address_;

  // Parsed addresses to advertise. Set by Start(). Empty if the bind addresses
  // should be advertised.
  std::vector<Sockaddr> webserver_advertised_addresses_;

  // Handle to Mongoose context; owned and freed by Mongoose internally
  struct sq_context* context_;
};

} // namespace kudu

#endif // KUDU_UTIL_WEBSERVER_H
