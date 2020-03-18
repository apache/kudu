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

#include "kudu/util/cloud/instance_metadata.h"

#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

// The timeout should be high enough to work effectively, but as low as possible
// to avoid slowing down detection of running in non-cloud environments. As of
// now, the metadata servers of major public cloud providers are robust enough
// to send the response in a fraction of a second.
DEFINE_uint32(cloud_metadata_server_request_timeout_ms, 1000,
              "Timeout for HTTP/HTTPS requests to the instance metadata server "
              "(in milliseconds)");
TAG_FLAG(cloud_metadata_server_request_timeout_ms, advanced);
TAG_FLAG(cloud_metadata_server_request_timeout_ms, runtime);

// The flags below are very unlikely to be customized since they are a part
// of the public API provided by the cloud providers. They are here for
// the peace of mind to be able to adapt for the changes in the cloud
// environment without the need to recompile the binaries.

// See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/\
//   ec2-instance-metadata.html#instancedata-data-retrieval
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/\
//   ec2-instance-metadata.html#instancedata-data-categories
DEFINE_string(cloud_aws_instance_id_url,
              "http://169.254.169.254/latest/meta-data/instance-id",
              "The URL to fetch the identifier of an AWS instance");
TAG_FLAG(cloud_aws_instance_id_url, advanced);
TAG_FLAG(cloud_aws_instance_id_url, runtime);

// See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html
// for details.
DEFINE_string(cloud_aws_ntp_server, "169.254.169.123",
              "IP address/FQDN of the internal NTP server to use from within "
              "an AWS instance");
TAG_FLAG(cloud_aws_ntp_server, advanced);
TAG_FLAG(cloud_aws_ntp_server, runtime);

// See https://docs.microsoft.com/en-us/azure/virtual-machines/linux/ \
//   instance-metadata-service#retrieving-metadata for details.
DEFINE_string(cloud_azure_instance_id_url,
              "http://169.254.169.254/metadata/instance/compute/vmId?"
              "api-version=2018-10-01&format=text",
              "The URL to fetch the identifier of an Azure instance");
TAG_FLAG(cloud_azure_instance_id_url, advanced);
TAG_FLAG(cloud_azure_instance_id_url, runtime);

// See https://cloud.google.com/compute/docs/instances/managing-instances# \
//   configure_ntp_for_your_instances for details.
DEFINE_string(cloud_gce_ntp_server, "metadata.google.internal",
              "IP address/FQDN of the internal NTP server to use from within "
              "a GCE instance");
TAG_FLAG(cloud_gce_ntp_server, advanced);
TAG_FLAG(cloud_gce_ntp_server, runtime);

// See https://cloud.google.com/compute/docs/storing-retrieving-metadata
// for details.
DEFINE_string(cloud_gce_instance_id_url,
              "http://metadata.google.internal/computeMetadata/v1/instance/id",
              "The URL to fetch the identifier of a GCE instance");
TAG_FLAG(cloud_gce_instance_id_url, advanced);
TAG_FLAG(cloud_gce_instance_id_url, runtime);

// See https://docs.openstack.org/nova/latest/user/metadata.html#metadata-service
// and https://docs.openstack.org/nova/latest/user/metadata.html#metadata-openstack-format
// for details.
DEFINE_string(cloud_openstack_metadata_url,
              "http://169.254.169.254/openstack/latest/meta_data.json",
              "The URL to fetch metadata of an OpenStack instance via Nova "
              "metadata service. OpenStack Nova metadata server does not "
              "provide a separate URL to fetch instance UUID, at least with "
              "12.0.0 (Liberty) release.");
TAG_FLAG(cloud_openstack_metadata_url, advanced);
TAG_FLAG(cloud_openstack_metadata_url, runtime);

DEFINE_validator(cloud_metadata_server_request_timeout_ms,
                 [](const char* name, const uint32_t val) {
  if (val == 0) {
    LOG(ERROR) << strings::Substitute(
        "unlimited timeout for metadata requests (value $0 for flag --$1) "
        "is not allowed", val, name);
    return false;
  }
  return true;
});

using std::string;
using std::vector;

namespace kudu {
namespace cloud {

const char* TypeToString(CloudType type) {
  static const char* const kTypeAws = "AWS";
  static const char* const kTypeAzure = "Azure";
  static const char* const kTypeGce = "GCE";
  static const char* const kTypeOpenStack = "OpenStack";
  switch (type) {
    case CloudType::AWS:
      return kTypeAws;
    case CloudType::AZURE:
      return kTypeAzure;
    case CloudType::GCE:
      return kTypeGce;
    case CloudType::OPENSTACK:
      return kTypeOpenStack;
    default:
      LOG(FATAL) << static_cast<uint16_t>(type) << ": unknown cloud type";
      break;  // unreachable
  }
}

InstanceMetadata::InstanceMetadata()
    : is_initialized_(false) {
}

Status InstanceMetadata::Init() {
  // As of now, fetching the instance identifier from metadata service is
  // the criterion for successful initialization of the instance metadata.
  DCHECK(!is_initialized_);
  RETURN_NOT_OK(Fetch(instance_id_url(), request_timeout(), request_headers()));
  is_initialized_ = true;
  return Status::OK();
}

MonoDelta InstanceMetadata::request_timeout() const {
  return MonoDelta::FromMilliseconds(
      FLAGS_cloud_metadata_server_request_timeout_ms);
}

Status InstanceMetadata::Fetch(const string& url,
                               MonoDelta timeout,
                               const vector<string>& headers,
                               string* out) {
  if (timeout.ToMilliseconds() == 0) {
    return Status::NotSupported(
        "unlimited timeout is not supported when retrieving instance metadata");
  }
  EasyCurl curl;
  curl.set_timeout(timeout);
  curl.set_fail_on_http_error(true);
  faststring resp;
  RETURN_NOT_OK(curl.FetchURL(url, &resp, headers));
  if (out) {
    *out = resp.ToString();
  }
  return Status::OK();
}

Status AwsInstanceMetadata::Init() {
  // Try if the metadata server speaks AWS API.
  RETURN_NOT_OK(InstanceMetadata::Init());

  // If OpenStack instance metadata server is configured to emulate EC2,
  // one way to tell it apart from a true EC2 instance is to check whether it
  // speaks OpenStack API:
  //   https://docs.openstack.org/nova/latest/user/metadata.html#metadata-ec2-format
  OpenStackInstanceMetadata openstack;
  if (openstack.Init().ok()) {
    return Status::ServiceUnavailable("found OpenStack instance, not AWS one");
  }
  return Status::OK();
}

Status AwsInstanceMetadata::GetNtpServer(string* server) const {
  DCHECK(server);
  *server = FLAGS_cloud_aws_ntp_server;
  return Status::OK();
}

const vector<string>& AwsInstanceMetadata::request_headers() const {
  // EC2 doesn't require any specific headers supplied with a generic query
  // to the metadata server.
  static const vector<string> kRequestHeaders = {};
  return kRequestHeaders;
}

const string& AwsInstanceMetadata::instance_id_url() const {
  return FLAGS_cloud_aws_instance_id_url;
}

Status AzureInstanceMetadata::GetNtpServer(string* /* server */) const {
  // An Azure instance doesn't have access to dedicated NTP servers: Azure
  // doesn't provide such a service.
  return Status::NotSupported("Azure doesn't provide a dedicated NTP server");
}

const vector<string>& AzureInstanceMetadata::request_headers() const {
  static const vector<string> kRequestHeaders = { "Metadata:true" };
  return kRequestHeaders;
}

const string& AzureInstanceMetadata::instance_id_url() const {
  return FLAGS_cloud_azure_instance_id_url;
}

Status GceInstanceMetadata::GetNtpServer(string* server) const {
  DCHECK(server);
  *server = FLAGS_cloud_gce_ntp_server;
  return Status::OK();
}

const vector<string>& GceInstanceMetadata::request_headers() const {
  static const vector<string> kHeaders = { "Metadata-Flavor:Google" };
  return kHeaders;
}

const string& GceInstanceMetadata::instance_id_url() const {
  return FLAGS_cloud_gce_instance_id_url;
}

Status OpenStackInstanceMetadata::GetNtpServer(string* /* server */) const {
  // OpenStack doesn't provide a dedicated NTP server for an instance.
  return Status::NotSupported("OpenStack doesn't provide a dedicated NTP server");
}

const vector<string>& OpenStackInstanceMetadata::request_headers() const {
  // OpenStack Nova doesn't require any specific headers supplied with a
  // generic query to the metadata server.
  static const vector<string> kRequestHeaders = {};
  return kRequestHeaders;
}

const string& OpenStackInstanceMetadata::instance_id_url() const {
  // NOTE: OpenStack Nova metadata server doesn't provide a separate URL to
  //   fetch ID of an instance (at least with 1.12.0 release):
  //   https://docs.openstack.org/nova/latest/user/metadata.html#metadata-openstack-format
  return FLAGS_cloud_openstack_metadata_url;
}

} // namespace cloud
} // namespace kudu
