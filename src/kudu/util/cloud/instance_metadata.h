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

#pragma once

#include <string>
#include <vector>

#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {
class MonoDelta;
}  // namespace kudu

namespace kudu {
namespace cloud {

enum class CloudType {
  AWS,
  AZURE,
  GCE,
};

const char* TypeToString(CloudType type);

// Generic interface to collect and access metadata of a public cloud instance.
// Concrete classes implementing this interface use stable APIs to retrieve
// corresponding information (published by corresponding cloud providers).
class InstanceMetadata {
 public:
  virtual ~InstanceMetadata() {}

  // Initialize the object, collecting information about a cloud instance.
  // It's a synchronous call and it can take some time to complete.
  // If the basic information has been retrieved successfully, returns
  // Status::OK(), otherwise returns non-OK status to reflect the error
  // encountered.
  virtual Status Init() WARN_UNUSED_RESULT = 0;

  // Get the type of the cloud instance.
  virtual CloudType type() const = 0;

  // Get identifier of the cloud instance.
  virtual const std::string& id() const = 0;

  // Get the internal NTP server accessible from within the instance.
  // On success, returns Status::OK() and populates the output parameter
  // 'server' with IP address or FQDN of the NTP server available from within
  // the instance. Returns
  //   * Status::NotSupported() if the cloud platform doesn't provide internal
  //                            NTP service for its instances
  //   * Status::IllegalState() if the metadata object requires initialization,
  //                            but it hasn't been initialized yet
  virtual Status GetNtpServer(std::string* server) const WARN_UNUSED_RESULT = 0;
};

// The common base class to work with the instance's metadata using the metadata
// server HTTP-based API. That's the ubiquitous way of accessing metadata from
// within a cloud instance (e.g., exists in AWS, GCE, DigitalOcean).
class InstanceMetadataBase : public InstanceMetadata {
 public:
  InstanceMetadataBase();
  ~InstanceMetadataBase() = default;

  Status Init() override WARN_UNUSED_RESULT;
  const std::string& id() const override;

 protected:
  // Fetch data from specified URL. Targeted for fetching information from
  // the instance's metadata server.
  static Status Fetch(const std::string& url,
                      MonoDelta timeout,
                      const std::vector<std::string>& headers,
                      std::string* out);

  // The timeout used for HTTP requests sent to the metadata server. The base
  // implementation assumes the metadata server is robust enough to respond
  // in a fraction of a second. If not, override this method accordingly.
  virtual MonoDelta request_timeout() const;

  // Return HTTP header fields to supply with requests to the metadata server.
  // Metadata servers might have specific requirements on expected headers.
  virtual const std::vector<std::string>& request_headers() const = 0;

  // Return metadata server's URL used to retrieve instance identifier.
  // It's assumed the server replies with plain text, where the string
  // contains just the identifier.
  virtual const std::string& instance_id_url() const = 0;

 private:
  // Fetch cloud instance identifier from the metadata server. Returns
  // Status::OK() in case of success, populating the output parameter 'id' with
  // the identifier of the instance. Returns non-OK status in case of errors.
  Status FetchInstanceId(std::string* id);

  // Instance identifier; valid only after successful initialization.
  std::string id_;

  // Whether this object has been initialized.
  bool is_initialized_;
};

// More information on the metadata server for EC2 cloud instances:
//   https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ \
//     ec2-instance-metadata.html
class AwsInstanceMetadata : public InstanceMetadataBase {
 public:
  AwsInstanceMetadata() = default;
  ~AwsInstanceMetadata() = default;

  CloudType type() const override { return CloudType::AWS; }
  Status GetNtpServer(std::string* server) const override WARN_UNUSED_RESULT;

 protected:
  const std::vector<std::string>& request_headers() const override;
  const std::string& instance_id_url() const override;
};

// More information on the metadata server for Azure cloud instances:
//   https://docs.microsoft.com/en-us/azure/virtual-machines/linux/ \
//     instance-metadata-service
class AzureInstanceMetadata : public InstanceMetadataBase {
 public:
  AzureInstanceMetadata() = default;
  ~AzureInstanceMetadata() = default;

  CloudType type() const override { return CloudType::AZURE; }
  Status GetNtpServer(std::string* server) const override WARN_UNUSED_RESULT;

 protected:
  const std::vector<std::string>& request_headers() const override;
  const std::string& instance_id_url() const override;
};

// More information on the metadata server for GCE cloud instances:
//   https://cloud.google.com/compute/docs/storing-retrieving-metadata
class GceInstanceMetadata : public InstanceMetadataBase {
 public:
  GceInstanceMetadata() = default;
  ~GceInstanceMetadata() = default;

  CloudType type() const override { return CloudType::GCE; }
  Status GetNtpServer(std::string* server) const override WARN_UNUSED_RESULT;

 protected:
  const std::vector<std::string>& request_headers() const override;
  const std::string& instance_id_url() const override;
};

} // namespace cloud
} // namespace kudu
