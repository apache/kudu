#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Verify that the hand-written Swagger spec matches the REST API handlers.
#
# Usage:
#   ./build-support/verify_swagger_spec.py
#
# Expected output:
# - Success:
#   Swagger verification successful.
# - Failure:
#   Swagger verification failed with N issue(s):
#   - <details>

import argparse
import json
import os
import re
import sys
from glob import glob


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
DEFAULT_SWAGGER_PATH = os.path.join(REPO_ROOT, "www", "swagger", "kudu-api.json")
DEFAULT_SOURCE_ROOT = os.path.join(REPO_ROOT, "src", "kudu")
DEFAULT_IGNORE_PATHS = ["/api/v1/spec", "/api/docs"]


# Matches RegisterPathHandler/RegisterPrerenderedPathHandler argument lists.
REGISTER_HANDLER_RE = re.compile(
    r"Register(?:Prerendered)?PathHandler\(\s*(?P<args>.*?)\);\s*",
    re.DOTALL,
)
# Extracts "/api/..." path literals from handler registrations.
PATH_RE = re.compile(r'"(?P<path>/api/[^"]+)"')
# Finds handler method names used in lambdas (e.g., this->HandleFoo).
HANDLER_RE = re.compile(r"this->(?P<handler>\w+)\s*\(")
# Detects explicit method checks (req.request_method == "GET").
METHOD_EQ_RE = re.compile(r'req\.request_method\s*==\s*"(?P<method>[A-Z]+)"')
# Detects negated method checks (req.request_method != "GET").
METHOD_NE_RE = re.compile(r'req\.request_method\s*!=\s*"(?P<method>[A-Z]+)"')
# Finds RestCatalogPathHandlers method definitions to scan for method checks.
FUNC_DEF_RE = re.compile(
    r"void\s+RestCatalogPathHandlers::(?P<name>\w+)\s*\([^)]*\)\s*\{",
    re.DOTALL,
)


def read_file(path):
  with open(path, "r", encoding="utf-8") as fh:
    return fh.read()


def normalize_base_path(base_path):
  if not base_path:
    return ""
  return "/" + base_path.strip("/")


def normalize_path(base_path, path):
  if not path.startswith("/"):
    path = "/" + path
  base = normalize_base_path(base_path)
  if base and path.startswith(base):
    return path
  return base + path


def swagger_operations(swagger):
  servers = swagger.get("servers", [])
  base_path = servers[0].get("url", "") if servers else ""
  paths = swagger.get("paths", {})
  operations = {}
  for swagger_path, path_item in paths.items():
    full_path = normalize_path(base_path, swagger_path)
    path_params = path_item.get("parameters", [])
    for method, spec in path_item.items():
      if method.lower() not in ("get", "put", "post", "delete", "patch", "head", "options"):
        continue
      op_params = list(path_params) + list(spec.get("parameters", []))
      op_path_params = {
          param["name"]
          for param in op_params
          if isinstance(param, dict) and param.get("in") == "path"
      }
      operations.setdefault(full_path, {})[method.upper()] = op_path_params
  return operations


def extract_block(text, start_idx):
  brace_count = 0
  in_block = False
  for idx in range(start_idx, len(text)):
    if text[idx] == "{":
      brace_count += 1
      in_block = True
    elif text[idx] == "}":
      brace_count -= 1
      if in_block and brace_count == 0:
        return text[start_idx:idx + 1]
  return ""


def extract_handler_methods(text, handler_name):
  for match in FUNC_DEF_RE.finditer(text):
    if match.group("name") != handler_name:
      continue
    body = extract_block(text, match.start())
    methods = set(METHOD_EQ_RE.findall(body))
    if methods:
      return methods
    method_ne = METHOD_NE_RE.search(body)
    if method_ne:
      return {method_ne.group("method")}
    return set()
  return None


def source_operations(source_root):
  source_paths = glob(os.path.join(source_root, "**", "*.cc"), recursive=True)
  operations = {}
  for path in source_paths:
    text = read_file(path)
    if "/api/" not in text or "Register" not in text:
      continue
    for match in REGISTER_HANDLER_RE.finditer(text):
      args = match.group("args")
      path_match = PATH_RE.search(args)
      handler_match = HANDLER_RE.search(args)
      if not path_match or not handler_match:
        continue
      endpoint_path = path_match.group("path")
      handler = handler_match.group("handler")
      methods = extract_handler_methods(text, handler)
      path_params = set(re.findall(r"<([^>]+)>", endpoint_path))
      normalized_path = re.sub(r"<([^>]+)>", r"{\1}", endpoint_path)
      operations[normalized_path] = {
          "methods": methods,
          "path_params": path_params,
          "handler": handler,
          "source": path,
      }
  return operations


def compare(swagger_ops, source_ops, ignore_paths):
  errors = []
  swagger_paths = set(swagger_ops.keys())
  source_paths = set(source_ops.keys()) - set(ignore_paths)

  extra_swagger = sorted(swagger_paths - source_paths)
  extra_source = sorted(source_paths - swagger_paths)

  for path in extra_swagger:
    errors.append("Swagger path missing in source: {}".format(path))
  for path in extra_source:
    errors.append("Source path missing in swagger: {}".format(path))

  for path in sorted(swagger_paths & source_paths):
    swagger_methods = set(swagger_ops[path].keys())
    source_methods = source_ops[path]["methods"]
    if source_methods is None:
      errors.append("Unable to locate handler for source path: {}".format(path))
      continue
    if not source_methods:
      errors.append(
          "Unable to infer HTTP methods for source path: {} "
          "(checker only scans direct req.request_method checks in the handler "
          "body; nested helpers are not detected)".format(path)
      )
      continue
    if swagger_methods != source_methods:
      missing = sorted(source_methods - swagger_methods)
      extra = sorted(swagger_methods - source_methods)
      if missing:
        errors.append("Swagger missing methods for {}: {}".format(path, ", ".join(missing)))
      if extra:
        errors.append("Swagger has extra methods for {}: {}".format(path, ", ".join(extra)))
    source_params = source_ops[path]["path_params"]
    for method, swagger_params in swagger_ops[path].items():
      if swagger_params != source_params:
        errors.append(
            "Parameter mismatch for {} {}: swagger={} source={}".format(
                method,
                path,
                sorted(swagger_params),
                sorted(source_params),
            )
        )
  return errors


def parse_args():
  parser = argparse.ArgumentParser(
      description="Verify Swagger spec matches REST API handlers."
  )
  parser.add_argument(
      "--swagger",
      default=DEFAULT_SWAGGER_PATH,
      help="Path to swagger json (default: %(default)s)",
  )
  parser.add_argument(
      "--source-root",
      default=DEFAULT_SOURCE_ROOT,
      help="Path to Kudu source root (default: %(default)s)",
  )
  parser.add_argument(
      "--ignore-path",
      action="append",
      default=list(DEFAULT_IGNORE_PATHS),
      help="API paths to ignore (repeatable)",
  )
  return parser.parse_args()


def main():
  args = parse_args()
  if not os.path.exists(args.swagger):
    print("Swagger spec not found: {}".format(args.swagger), file=sys.stderr)
    return 2
  swagger = json.loads(read_file(args.swagger))
  swagger_ops = swagger_operations(swagger)
  if not swagger_ops:
    print("No swagger operations found in {}".format(args.swagger), file=sys.stderr)
    return 2
  source_ops = source_operations(args.source_root)
  if not source_ops:
    print("No REST handlers found under {}".format(args.source_root), file=sys.stderr)
    return 2
  errors = compare(swagger_ops, source_ops, args.ignore_path)
  if errors:
    print("Swagger verification failed with {} issue(s):".format(len(errors)))
    for error in errors:
      print("- {}".format(error))
    return 1
  print("Swagger verification successful.")
  return 0


if __name__ == "__main__":
  sys.exit(main())
