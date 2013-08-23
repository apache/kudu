// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef KUDU_UTIL_WEBSERVER_H
#define KUDU_UTIL_WEBSERVER_H

#include <string>
#include <map>
#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>

#include "util/net/sockaddr.h"
#include "util/status.h"

struct mg_connection;
struct mg_request_info;
struct mg_context;

namespace kudu {

// Wrapper class for the Mongoose web server library. Clients may register callback
// methods which produce output for a given URL path
class Webserver {
 public:
  typedef std::map<std::string, std::string> ArgumentMap;
  typedef boost::function<void (const ArgumentMap& args, std::stringstream* output)>
      PathHandlerCallback;

  // Using this constructor, the webserver will bind to all available
  // interfaces.
  explicit Webserver(const int port);

  // Uses FLAGS_webserver_{port, interface}
  Webserver();

  ~Webserver();

  // Starts a webserver on the port passed to the constructor. The webserver runs in a
  // separate thread, so this call is non-blocking.
  Status Start();

  // Stops the webserver synchronously.
  void Stop();

  // Register a callback for a URL path. Path should not include the
  // http://hostname/ prefix. If is_styled is true, the page is meant to be for
  // people to look at and is styled.  If false, it is meant to be for machines to
  // scrape.  If is_on_nav_bar is true,  a link to this page is
  // printed in the navigation bar at the top of each debug page. Otherwise the
  // link does not appear, and the page is rendered without HTML headers and
  // footers.
  // The first registration's choice of is_styled overrides all
  // subsequent registrations for that URL.
  void RegisterPathHandler(const std::string& path, const PathHandlerCallback& callback,
                           bool is_styled = true, bool is_on_nav_bar = true);

  // True if serving all traffic over SSL, false otherwise
  bool IsSecure() const;
 private:
  // Container class for a list of path handler callbacks for a single URL.
  class PathHandler {
   public:
    PathHandler(bool is_styled, bool is_on_nav_bar)
        : is_styled_(is_styled), is_on_nav_bar_(is_on_nav_bar) {}

    void AddCallback(const PathHandlerCallback& callback) {
      callbacks_.push_back(callback);
    }

    bool is_styled() const { return is_styled_; }
    bool is_on_nav_bar() const { return is_on_nav_bar_; }
    const std::vector<PathHandlerCallback>& callbacks() const { return callbacks_; }

   private:
    // If true, the page appears is rendered styled.
    bool is_styled_;

    // If true, the page appears in the navigation bar.
    bool is_on_nav_bar_;

    // List of callbacks to render output for this page, called in order.
    std::vector<PathHandlerCallback> callbacks_;
  };

  // Build the string to pass to mongoose specifying where to bind.
  Status BuildListenSpec(std::string* spec) const;

  // Renders a common Bootstrap-styled header
  void BootstrapPageHeader(std::stringstream* output);

  // Renders a common Bootstrap-styled footer. Must be used in conjunction with
  // BootstrapPageHeader.
  void BootstrapPageFooter(std::stringstream* output);

  // Dispatch point for all incoming requests.
  // Static so that it can act as a function pointer, and then call the next method
  static int BeginRequestCallbackStatic(struct mg_connection* connection);
  int BeginRequestCallback(struct mg_connection* connection,
                           struct mg_request_info* request_info);

  // Callback to funnel mongoose logs through glog.
  static int LogMessageCallbackStatic(const struct mg_connection* connection,
                                      const char* message);

  // Registered to handle "/", and prints a list of available URIs
  void RootHandler(const ArgumentMap& args, std::stringstream* output);

  // Builds a map of argument name to argument value from a typical URL argument
  // string (that is, "key1=value1&key2=value2.."). If no value is given for a
  // key, it is entered into the map as (key, "").
  void BuildArgumentMap(const std::string& args, ArgumentMap* output);

  // Lock guarding the path_handlers_ map
  boost::mutex path_handlers_lock_;

  // Map of path to a PathHandler containing a list of handlers for that
  // path. More than one handler may register itself with a path so that many
  // components may contribute to a single page.
  typedef std::map<std::string, PathHandler> PathHandlerMap;
  PathHandlerMap path_handlers_;

  // The address of the interface on which to run this webserver.
  std::string http_address_;

  // Handle to Mongoose context; owned and freed by Mongoose internally
  struct mg_context* context_;
};

}

#endif // KUDU_UTIL_WEBSERVER_H
