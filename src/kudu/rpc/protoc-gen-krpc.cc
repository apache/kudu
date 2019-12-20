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

////////////////////////////////////////////////////////////////////////////////
// Example usage:
// protoc --plugin=protoc-gen-krpc --krpc_out . --proto_path . <file>.proto
////////////////////////////////////////////////////////////////////////////////

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/rpc/rpc_header.pb.h"

using boost::optional;
using google::protobuf::FileDescriptor;
using google::protobuf::io::Printer;
using google::protobuf::MethodDescriptor;
using google::protobuf::ServiceDescriptor;
using std::map;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace kudu {
namespace rpc {

namespace {

// Return the name of the authorization method specified for this
// RPC method, or boost::none if none is specified.
//
// This handles fallback to the service-wide default.
optional<string> GetAuthzMethod(const MethodDescriptor& method) {
  if (method.options().HasExtension(authz_method)) {
    return method.options().GetExtension(authz_method);
  }
  if (method.service()->options().HasExtension(default_authz_method)) {
    return method.service()->options().GetExtension(default_authz_method);
  }
  return boost::none;
}

} // anonymous namespace

class Substituter {
 public:
  virtual ~Substituter() {}
  virtual void InitSubstitutionMap(map<string, string>* map) const = 0;
};

// NameInfo contains information about the output names.
class FileSubstitutions : public Substituter {
 public:
  bool Init(const FileDescriptor* file, string* error) {
    static const char* kProtoExtension = ".proto";

    // If path = /foo/bar/baz_stuff.proto, path_no_extension = /foo/bar/baz_stuff
    const string& path = file->name();
    if (!TryStripSuffixString(path, kProtoExtension, &path_no_extension_)) {
      *error = Substitute("name_info.Init failed: file name $0 did not end in $1",
                          path, kProtoExtension);
      return false;
    }
    map_["path"] = path;
    map_["path_no_extension"] = path_no_extension_;

    map_["open_namespace"] = GenerateOpenNamespace(file->package());
    map_["close_namespace"] = GenerateCloseNamespace(file->package());

    return true;
  }

  void InitSubstitutionMap(map<string, string>* map) const override {
    for (const auto& pair : map_) {
      (*map)[pair.first] = pair.second;
    }
  }

  string service_header() const {
    return path_no_extension_ + ".service.h";
  }

  string service() const {
    return path_no_extension_ + ".service.cc";
  }

  string proxy_header() const {
    return path_no_extension_ + ".proxy.h";
  }

  string proxy() const {
    return path_no_extension_ + ".proxy.cc";
  }

 private:
  static string GenerateOpenNamespace(const string& str) {
    vector<string> components = Split(str, ".");
    string out;
    for (const auto& c : components) {
      out.append("namespace ").append(c).append(" {\n");
    }
    return out;
  }

  static string GenerateCloseNamespace(const string& str) {
    vector<string> components = Split(str, ".");
    string out;
    for (auto c = components.crbegin(); c != components.crend(); c++) {
      out.append("} // namespace ").append(*c).append("\n");
    }
    return out;
  }

  string path_no_extension_;
  map<string, string> map_;
};

class MethodSubstitutions : public Substituter {
 public:
  explicit MethodSubstitutions(const MethodDescriptor* method)
    : method_(method) {
  }

  void InitSubstitutionMap(map<string, string>* map) const override {
    (*map)["rpc_name"] = method_->name();
    (*map)["rpc_full_name"] = method_->full_name();
    (*map)["rpc_full_name_plainchars"] =
        StringReplace(method_->full_name(), ".", "_", true);
    (*map)["request"] =
        ReplaceNamespaceDelimiters(
            StripNamespaceIfPossible(method_->service()->full_name(),
                                     method_->input_type()->full_name()));
    (*map)["response"] =
        ReplaceNamespaceDelimiters(
            StripNamespaceIfPossible(method_->service()->full_name(),
                                     method_->output_type()->full_name()));
    (*map)["metric_enum_key"] = Substitute("kMetricIndex$0", method_->name());
    bool track_result = static_cast<bool>(method_->options().GetExtension(track_rpc_result));
    (*map)["track_result"] = track_result ? " true" : "false";
    (*map)["authz_method"] = GetAuthzMethod(*method_).get_value_or("AuthorizeAllowAll");
  }

  // Strips the package from method arguments if they are in the same package as
  // the service, otherwise leaves them so that we can have fully qualified
  // namespaces for method arguments.
  static string StripNamespaceIfPossible(const string& service_full_name,
                                         const string& arg_full_name) {
    StringPiece service_package(service_full_name);
    if (!service_package.contains(".")) {
      return arg_full_name;
    }
    // remove the service name so that we are left with only the package, including
    // the last '.' so that we account for different packages with the same prefix.
    service_package.remove_suffix(service_package.length() -
                                  service_package.find_last_of(".") - 1);

    StringPiece argfqn(arg_full_name);
    if (argfqn.starts_with(service_package)) {
      argfqn.remove_prefix(argfqn.find_last_of(".") + 1);
    }
    return argfqn.ToString();
  }

  static string ReplaceNamespaceDelimiters(const string& arg_full_name) {
    return JoinStrings(strings::Split(arg_full_name, "."), "::");
  }

 private:
  const MethodDescriptor* method_;
};

class ServiceSubstitutions : public Substituter {
 public:
  explicit ServiceSubstitutions(const ServiceDescriptor* service)
    : service_(service)
  {}

  void InitSubstitutionMap(map<string, string>* map) const override {
    (*map)["service_name"] = service_->name();
    (*map)["full_service_name"] = service_->full_name();
    (*map)["service_method_count"] = SimpleItoa(service_->method_count());

    // TODO: upgrade to protobuf 2.5.x and attach service comments
    // to the generated service classes using the SourceLocation API.
  }

 private:
  const ServiceDescriptor* service_;
};


class SubstitutionContext {
 public:
  // Takes ownership of the substituter.
  void Push(unique_ptr<const Substituter> sub) {
    subs_.emplace_back(std::move(sub));
  }

  void PushMethod(const MethodDescriptor* method) {
    Push(unique_ptr<const Substituter>(new MethodSubstitutions(method)));
  }

  void PushService(const ServiceDescriptor* service) {
    Push(unique_ptr<const Substituter>(new ServiceSubstitutions(service)));
  }

  void Pop() {
    CHECK(!subs_.empty());
    subs_.pop_back();
  }

  void InitSubstitutionMap(map<string, string>* subs) const {
    for (const auto& sub : subs_) {
      sub->InitSubstitutionMap(subs);
    }
  }

 private:
  vector<unique_ptr<const Substituter>> subs_;
};



class CodeGenerator : public ::google::protobuf::compiler::CodeGenerator {
 public:
  CodeGenerator() { }

  ~CodeGenerator() { }

  bool Generate(const google::protobuf::FileDescriptor* file,
        const string&/* parameter */,
        google::protobuf::compiler::GeneratorContext* gen_context,
        string* error) const override {
    unique_ptr<FileSubstitutions> name_info(new FileSubstitutions());
    bool ret = name_info->Init(file, error);
    if (!ret) {
      return false;
    }

    // 'subs' takes ownership but we need to keep using it.
    const FileSubstitutions* name_info_ptr = name_info.get();

    SubstitutionContext subs;
    subs.Push(std::move(name_info));

    unique_ptr<google::protobuf::io::ZeroCopyOutputStream> ih_output(
        gen_context->Open(name_info_ptr->service_header()));
    Printer ih_printer(ih_output.get(), '$');
    GenerateServiceIfHeader(&ih_printer, &subs, file);

    unique_ptr<google::protobuf::io::ZeroCopyOutputStream> i_output(
        gen_context->Open(name_info_ptr->service()));
    Printer i_printer(i_output.get(), '$');
    GenerateServiceIf(&i_printer, &subs, file);

    unique_ptr<google::protobuf::io::ZeroCopyOutputStream> ph_output(
        gen_context->Open(name_info_ptr->proxy_header()));
    Printer ph_printer(ph_output.get(), '$');
    GenerateProxyHeader(&ph_printer, &subs, file);

    unique_ptr<google::protobuf::io::ZeroCopyOutputStream> p_output(
        gen_context->Open(name_info_ptr->proxy()));
    Printer p_printer(p_output.get(), '$');
    GenerateProxy(&p_printer, &subs, file);

    return true;
  }

 private:
  static void Print(Printer* printer,
                    const SubstitutionContext& sub,
                    const char* text) {
    map<string, string> subs;
    sub.InitSubstitutionMap(&subs);
    printer->Print(subs, text);
  }

  static void GenerateServiceIfHeader(Printer* printer,
                                      SubstitutionContext* subs,
                                      const FileDescriptor* file) {
    Print(printer, *subs,
      "// THIS FILE IS AUTOGENERATED FROM $path$\n"
      "\n"
      "#pragma once\n"
      "\n"
      "#include <string>\n"
      "\n"
      "#include \"kudu/gutil/ref_counted.h\"\n"
      "#include \"kudu/rpc/service_if.h\"\n"
      "\n"
      "namespace google {\n"
      "namespace protobuf {\n"
      "class Message;\n"
      "} // namespace protobuf\n"
      "} // namespace google\n"
      "\n"
      "namespace kudu {\n"
      "class MetricEntity;\n"
      "namespace rpc {\n"
      "class ResultTracker;\n"
      "class RpcContext;\n"
      "} // namespace rpc\n"
      "} // namespace kudu\n"
      "\n"
      "$open_namespace$"
      "\n");

    for (int service_idx = 0; service_idx < file->service_count();
         ++service_idx) {
      const ServiceDescriptor* service = file->service(service_idx);
      subs->PushService(service);

      Print(printer, *subs,
        "class $service_name$If : public ::kudu::rpc::GeneratedServiceIf {\n"
        " public:\n"
        "  explicit $service_name$If(const scoped_refptr<::kudu::MetricEntity>& entity,"
            " const scoped_refptr<::kudu::rpc::ResultTracker>& result_tracker);\n"
        "  virtual ~$service_name$If();\n"
        "  std::string service_name() const override;\n"
        "  static std::string static_service_name();\n"
        "\n"
        );

      set<string> authz_methods;
      for (int method_idx = 0; method_idx < service->method_count();
           ++method_idx) {
        const MethodDescriptor *method = service->method(method_idx);
        subs->PushMethod(method);

        Print(printer, *subs,
        "  virtual void $rpc_name$(const class $request$ *req,\n"
        "      class $response$ *resp, ::kudu::rpc::RpcContext *context) = 0;\n"
        );
        subs->Pop(); // method
        if (auto m = GetAuthzMethod(*method)) {
          authz_methods.insert(m.get());
        }
      }

      if (!authz_methods.empty()) {
        printer->Print(
        "\n\n"
        "  // Authorization methods\n"
        "  // ---------------------\n\n");
      }
      for (const string& m : authz_methods) {
        printer->Print({ {"m", m} },
        "  virtual bool $m$(const google::protobuf::Message* req,\n"
        "     google::protobuf::Message* resp, ::kudu::rpc::RpcContext *context) = 0;\n");
      }

      Print(printer, *subs,
        "\n"
        "};\n"
      );

      subs->Pop(); // Service
    }

    Print(printer, *subs,
      "\n"
      "$close_namespace$"
      "\n");
  }

  static void GenerateServiceIf(Printer* printer,
                                SubstitutionContext* subs,
                                const FileDescriptor* file) {
    Print(printer, *subs,
      "// THIS FILE IS AUTOGENERATED FROM $path$\n"
      "\n"
      "#include <functional>\n"
      "#include <memory>\n"
      "#include <unordered_map>\n"
      "#include <utility>\n"
      "\n"
      "#include <google/protobuf/message.h>\n"
      "\n"
      "#include \"$path_no_extension$.pb.h\"\n"
      "#include \"$path_no_extension$.service.h\"\n"
      "\n"
      "#include \"kudu/rpc/result_tracker.h\"\n"
      "#include \"kudu/rpc/service_if.h\"\n"
      "#include \"kudu/util/metrics.h\"\n"
      "\n");

    // Define metric prototypes for each method in the service.
    for (int service_idx = 0; service_idx < file->service_count();
        ++service_idx) {
      const ServiceDescriptor* service = file->service(service_idx);
      subs->PushService(service);

      for (int method_idx = 0; method_idx < service->method_count();
          ++method_idx) {
        const MethodDescriptor* method = service->method(method_idx);
        subs->PushMethod(method);
        Print(printer, *subs,
          "METRIC_DEFINE_histogram(server, handler_latency_$rpc_full_name_plainchars$,\n"
          "  \"$rpc_full_name$ RPC Time\",\n"
          "  kudu::MetricUnit::kMicroseconds,\n"
          "  \"Microseconds spent handling $rpc_full_name$() RPC requests\",\n"
          "  kudu::MetricLevel::kInfo,\n"
          "  60000000LU, 2);\n"
          "\n");
        subs->Pop(); // method
      }

      subs->Pop(); // service
    }

    Print(printer, *subs,
      "using google::protobuf::Message;\n"
      "using kudu::MetricEntity;\n"
      "using kudu::rpc::ResultTracker;\n"
      "using kudu::rpc::RpcContext;\n"
      "using kudu::rpc::RpcMethodInfo;\n"
      "using std::unique_ptr;\n"
      "\n"
      "$open_namespace$"
      "\n");

    for (int service_idx = 0; service_idx < file->service_count();
         ++service_idx) {
      const ServiceDescriptor* service = file->service(service_idx);
      subs->PushService(service);

      Print(printer, *subs,
        "$service_name$If::$service_name$If(const scoped_refptr<MetricEntity>& entity,"
            " const scoped_refptr<ResultTracker>& result_tracker) {\n"
            "result_tracker_ = result_tracker;\n"
      );
      for (int method_idx = 0; method_idx < service->method_count();
           ++method_idx) {
        const MethodDescriptor* method = service->method(method_idx);
        subs->PushMethod(method);

        Print(printer, *subs,
              "  {\n"
              "    scoped_refptr<RpcMethodInfo> mi(new RpcMethodInfo());\n"
              "    mi->req_prototype.reset(new $request$());\n"
              "    mi->resp_prototype.reset(new $response$());\n"
              "    mi->authz_method = [this](const Message* req, Message* resp,\n"
              "                              RpcContext* ctx) {\n"
              "      return this->$authz_method$(static_cast<const $request$*>(req),\n"
              "                           static_cast<$response$*>(resp),\n"
              "                           ctx);\n"
              "    };\n"
              "    mi->track_result = $track_result$;\n"
              "    mi->handler_latency_histogram =\n"
              "        METRIC_handler_latency_$rpc_full_name_plainchars$.Instantiate(entity);\n"
              "    mi->func = [this](const Message* req, Message* resp, RpcContext* ctx) {\n"
              "      this->$rpc_name$(static_cast<const $request$*>(req),\n"
              "                       static_cast<$response$*>(resp),\n"
              "                       ctx);\n"
              "    };\n"
              "    methods_by_name_[\"$rpc_name$\"] = std::move(mi);\n"
              "  }\n");
        subs->Pop(); // method
      }

      Print(printer, *subs,
        "}\n"
        "\n"
        "$service_name$If::~$service_name$If() {\n"
        "}\n"
        "\n"
        "std::string $service_name$If::service_name() const {\n"
        "  return \"$full_service_name$\";\n"
        "}\n"
        "std::string $service_name$If::static_service_name() {\n"
        "  return \"$full_service_name$\";\n"
        "}\n"
        "\n"
      );

      subs->Pop(); // service
    }

    Print(printer, *subs,
      "\n"
      "$close_namespace$"
      "\n"
      );
  }

  static void GenerateProxyHeader(Printer* printer,
                                  SubstitutionContext* subs,
                                  const FileDescriptor* file) {
    Print(printer, *subs,
      "// THIS FILE IS AUTOGENERATED FROM $path$\n"
      "\n"
      "#pragma once\n"
      "\n"
      "#include <memory>\n"
      "#include <string>\n"
      "\n"
      "#include \"kudu/rpc/proxy.h\"\n"
      "#include \"kudu/rpc/response_callback.h\"\n"
      "#include \"kudu/util/status.h\"\n"
      "\n"
      "namespace kudu { class Sockaddr; }\n"
      "namespace kudu {\n"
      "namespace rpc {\n"
      "class Messenger;\n"
      "class RpcController;\n"
      "} // namespace rpc\n"
      "} // namespace kudu\n"
      "\n"
      "$open_namespace$"
      "\n");

    for (int service_idx = 0; service_idx < file->service_count();
         ++service_idx) {
      const ServiceDescriptor* service = file->service(service_idx);
      subs->PushService(service);

      Print(printer, *subs,
        "class $service_name$Proxy : public ::kudu::rpc::Proxy {\n"
        " public:\n"
        "  $service_name$Proxy(std::shared_ptr<::kudu::rpc::Messenger>\n"
        "                messenger, const ::kudu::Sockaddr &sockaddr,"
        "                std::string hostname);\n"
        "  ~$service_name$Proxy();\n"
        "\n"
        );

      for (int method_idx = 0; method_idx < service->method_count();
           ++method_idx) {
        const MethodDescriptor* method = service->method(method_idx);
        subs->PushMethod(method);

        Print(printer, *subs,
        "\n"
        "  ::kudu::Status $rpc_name$(const class $request$ &req,\n"
        "                            class $response$ *resp,\n"
        "                            ::kudu::rpc::RpcController *controller);\n"
        "  void $rpc_name$Async(const class $request$ &req,\n"
        "                       class $response$ *response,\n"
        "                       ::kudu::rpc::RpcController *controller,\n"
        "                       const ::kudu::rpc::ResponseCallback &callback);\n"
        );
        subs->Pop(); // method
      }
      Print(printer, *subs,
      "};\n");
      subs->Pop(); // service
    }
    Print(printer, *subs,
      "\n"
      "$close_namespace$"
      "\n"
    );
  }

  static void GenerateProxy(Printer* printer,
                            SubstitutionContext* subs,
                            const FileDescriptor* file) {
    Print(printer, *subs,
      "// THIS FILE IS AUTOGENERATED FROM $path$\n"
      "\n"
      "#include <string>\n"
      "#include <utility>\n"
      "\n"
      "#include \"$path_no_extension$.pb.h\"\n"
      "#include \"$path_no_extension$.proxy.h\"\n"
      "\n"
      "namespace kudu {\n"
      "namespace rpc {\n"
      "class Messenger;\n"
      "class RpcController;\n"
      "} // namespace rpc\n"
      "} // namespace kudu\n"
      "\n"
      "$open_namespace$"
      "\n");

    for (int service_idx = 0; service_idx < file->service_count();
         ++service_idx) {
      const ServiceDescriptor* service = file->service(service_idx);
      subs->PushService(service);
      Print(printer, *subs,
        "$service_name$Proxy::$service_name$Proxy(\n"
        "   std::shared_ptr< ::kudu::rpc::Messenger> messenger,\n"
        "   const ::kudu::Sockaddr &remote, std::string hostname)\n"
        "  : Proxy(std::move(messenger), remote, std::move(hostname), \"$full_service_name$\") {\n"
        "}\n"
        "\n"
        "$service_name$Proxy::~$service_name$Proxy() {\n"
        "}\n"
        "\n"
        "\n");
      for (int method_idx = 0; method_idx < service->method_count();
           ++method_idx) {
        const MethodDescriptor* method = service->method(method_idx);
        subs->PushMethod(method);
        Print(printer, *subs,
        "::kudu::Status $service_name$Proxy::$rpc_name$(const $request$ &req, $response$ *resp,\n"
        "                                     ::kudu::rpc::RpcController *controller) {\n"
        "  return SyncRequest(\"$rpc_name$\", req, resp, controller);\n"
        "}\n"
        "\n"
        "void $service_name$Proxy::$rpc_name$Async(const $request$ &req,\n"
        "                     $response$ *resp, ::kudu::rpc::RpcController *controller,\n"
        "                     const ::kudu::rpc::ResponseCallback &callback) {\n"
        "  AsyncRequest(\"$rpc_name$\", req, resp, controller, callback);\n"
        "}\n"
        "\n");
        subs->Pop(); // method
      }

      subs->Pop(); // service
    }
    Print(printer, *subs,
      "\n"
      "$close_namespace$"
      "\n");
  }
};
} // namespace rpc
} // namespace kudu

int main(int argc, char* argv[]) {
  kudu::rpc::CodeGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
