#!/usr/local/bin/thrift -java

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

# DO NOT MODIFY! Copied from
# https://raw.githubusercontent.com/apache/sentry/release-2.0.1/sentry-provider/sentry-provider-db/src/main/resources/sentry_policy_service.thrift
#
# With edits:
#   - Change cpp namespace to 'sentry' to match the Kudu codebase style.

#
# Thrift Service that the MetaStore is built on
#

include "sentry_common_service.thrift"

namespace java org.apache.sentry.provider.db.service.thrift
namespace php sentry.provider.db.service.thrift
namespace cpp sentry

enum TSentryGrantOption {
  TRUE = 1,
  FALSE = 0,
  # UNSET is used for revoke privilege, the component like 'hive'
  # didn't support getting grant option, so use UNSET is stand
  # for revoke both privileges with grant option and without grant
  # option.
  UNSET = -1
}

# Represents a Privilege in transport from the client to the server
struct TSentryPrivilege {
1: required string privilegeScope, # Valid values are SERVER, DATABASE, TABLE, COLUMN, URI
3: required string serverName,
4: optional string dbName = "",
5: optional string tableName = "",
6: optional string URI = "",
7: required string action = "",
8: optional i64 createTime, # Set on server side
9: optional TSentryGrantOption grantOption = TSentryGrantOption.FALSE
10: optional string columnName = "",
}

# TODO can this be deleted? it's not adding value to TAlterSentryRoleAddGroupsRequest
struct TSentryGroup {
1: required string groupName
}

# CREATE ROLE r1
struct TCreateSentryRoleRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required string roleName, # TSentryRole is not required for this request
}
struct TCreateSentryRoleResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

# DROP ROLE r1
struct TDropSentryRoleRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required string roleName # role to drop
}
struct TDropSentryRoleResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

# GRANT ROLE r1 TO GROUP g1
struct TAlterSentryRoleAddGroupsRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required string roleName,
5: required set<TSentryGroup> groups
}

struct TAlterSentryRoleAddGroupsResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

# GRANT ROLE r1 TO USER u1
struct TAlterSentryRoleAddUsersRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required string roleName,
4: required set<string> users
}

struct TAlterSentryRoleAddUsersResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

# REVOKE ROLE r1 FROM GROUP g1
struct TAlterSentryRoleDeleteGroupsRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required string roleName,
5: required set<TSentryGroup> groups
}
struct TAlterSentryRoleDeleteGroupsResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

# REVOKE ROLE r1 FROM USER u1
struct TAlterSentryRoleDeleteUsersRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required string roleName,
4: required set<string> users
}
struct TAlterSentryRoleDeleteUsersResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

# GRANT ... ON ... TO ROLE ...
struct TAlterSentryRoleGrantPrivilegeRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required string roleName,
5: optional TSentryPrivilege privilege,
6: optional set<TSentryPrivilege> privileges
}
struct TAlterSentryRoleGrantPrivilegeResponse {
1: required sentry_common_service.TSentryResponseStatus status
2: optional TSentryPrivilege privilege
3: optional set<TSentryPrivilege> privileges
}

# REVOKE ... ON ... FROM ROLE ...
struct TAlterSentryRoleRevokePrivilegeRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required string roleName,
5: optional TSentryPrivilege privilege,
6: optional set<TSentryPrivilege> privileges
}
struct TAlterSentryRoleRevokePrivilegeResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

# SHOW ROLE GRANT
struct TListSentryRolesRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string requestorUserName, # user on whose behalf the request is issued
3: optional string groupName # for this group, or all roles for all groups if null
}

struct TListSentryRolesForUserRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required string userName
}

# used only for TListSentryRolesResponse
struct TSentryRole {
1: required string roleName,
2: required set<TSentryGroup> groups,
3: required string grantorPrincipal #Deprecated
}
struct TListSentryRolesResponse {
1: required sentry_common_service.TSentryResponseStatus status
2: optional set<TSentryRole> roles
}

struct TSentryAuthorizable {
1: required string server,
2: optional string uri,
3: optional string db,
4: optional string table,
5: optional string column,
}

# SHOW GRANT
struct TListSentryPrivilegesRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string requestorUserName, # user on whose behalf the request is issued
4: required string roleName, # get privileges assigned for this role
5: optional TSentryAuthorizable authorizableHierarchy # get privileges assigned for this role
}
struct TListSentryPrivilegesResponse {
1: required sentry_common_service.TSentryResponseStatus status
2: optional set<TSentryPrivilege> privileges
}

# Drop privilege
struct TDropPrivilegesRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required TSentryAuthorizable authorizable
}

struct TDropPrivilegesResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

struct TRenamePrivilegesRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required TSentryAuthorizable oldAuthorizable
4: required TSentryAuthorizable newAuthorizable
}

struct TRenamePrivilegesResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

# This API was created specifically for ProviderBackend.getPrivileges
# and is not mean for general purpose privilege retrieval.
# This request/response pair are created specifically so we can
# efficiently obtain the specific privilges for a user query
struct TSentryActiveRoleSet {
1: required bool all,
2: required set<string> roles,
}
struct TListSentryPrivilegesForProviderRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required set<string> groups,
3: required TSentryActiveRoleSet roleSet,
4: optional TSentryAuthorizable authorizableHierarchy,
5: optional set<string> users
}
struct TListSentryPrivilegesForProviderResponse {
1: required sentry_common_service.TSentryResponseStatus status
2: required set<string> privileges
}

# List role:set<privileges> for the given authorizable
# Optionally use the set of groups to filter the roles
struct TSentryPrivilegeMap {
1: required map<string, set<TSentryPrivilege>> privilegeMap
}
struct TListSentryPrivilegesByAuthRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required set<TSentryAuthorizable> authorizableSet,
4: optional set<string> groups,
5: optional TSentryActiveRoleSet roleSet
}
struct TListSentryPrivilegesByAuthResponse {
1: required sentry_common_service.TSentryResponseStatus status,
2: optional map<TSentryAuthorizable, TSentryPrivilegeMap> privilegesMapByAuth # will not be set in case of an error
}

# Obtain a config value from the Sentry service
struct TSentryConfigValueRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required string propertyName, # Config attribute to obtain
3: optional string defaultValue # Value if propertyName not found
}
struct TSentryConfigValueResponse {
1: required sentry_common_service.TSentryResponseStatus status
2: optional string value
}

# struct for the mapping data like group to role, role to privilege
struct TSentryMappingData {
1: optional map<string, set<string>> groupRolesMap,                # for the groupName -> role mapping
2: optional map<string, set<TSentryPrivilege>>  rolePrivilegesMap, # for the roleName -> privilege mapping
3: optional map<string, set<string>> userRolesMap                  # for the userName -> role mapping
}

struct TSentryExportMappingDataRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName, # user on whose behalf the request is issued
3: optional string objectPath # for specific auth object
}

struct TSentryExportMappingDataResponse {
1: required sentry_common_service.TSentryResponseStatus status,
2: required TSentryMappingData mappingData
}

struct TSentryImportMappingDataRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V1,
2: required string requestorUserName, # user on whose behalf the request is issued
3: required bool overwriteRole = false, # if overwrite the exist role with the imported privileges, default is false 
4: required TSentryMappingData mappingData
}

struct TSentryImportMappingDataResponse {
1: required sentry_common_service.TSentryResponseStatus status
}

/*
 * API for synchronizing between HMS notification events and Sentry.
 *
 * When Sentry gets updates from HMS using HMS Notifications, HMS should
 * should wait after each notification event is generated until the notification
 * is handled by Sentry This preserves the synchronous semantics of DDL statements.
 *
 * The notification synchronization API is private between HMS and Sentry and should
 * not be used by anything else.
 *
 * The API should be used in the following way:
 *
 * 1) HMS creates a notification and stores its ID in the persistent storage
 * 2) HMS sends ID to Sentry
 * 3) Sentry blocks the response until the specified ID is processed by Sentry
 * 4) Sentry responds with the most recent processed ID.
 *
 * Note that the important part is blocking in Sentry until the specified ID
 * is processed. The returned most recent processed ID is intended for debugging
 * purposes only, but may be used in HMS for performance optimizations.
 */

struct TSentrySyncIDRequest {
1: required i32 protocol_version = sentry_common_service.TSENTRY_SERVICE_V2,
2: required i64 id // Requested ID
}

struct TSentrySyncIDResponse {
1: required sentry_common_service.TSentryResponseStatus status
2: required i64 id // Most recent processed ID
}

service SentryPolicyService
{
  TCreateSentryRoleResponse create_sentry_role(1:TCreateSentryRoleRequest request)
  TDropSentryRoleResponse drop_sentry_role(1:TDropSentryRoleRequest request)

  TAlterSentryRoleGrantPrivilegeResponse alter_sentry_role_grant_privilege(1:TAlterSentryRoleGrantPrivilegeRequest request)
  TAlterSentryRoleRevokePrivilegeResponse alter_sentry_role_revoke_privilege(1:TAlterSentryRoleRevokePrivilegeRequest request)

  TAlterSentryRoleAddGroupsResponse alter_sentry_role_add_groups(1:TAlterSentryRoleAddGroupsRequest request)
  TAlterSentryRoleDeleteGroupsResponse alter_sentry_role_delete_groups(1:TAlterSentryRoleDeleteGroupsRequest request)

  TAlterSentryRoleAddUsersResponse alter_sentry_role_add_users(1:TAlterSentryRoleAddUsersRequest request)
  TAlterSentryRoleDeleteUsersResponse alter_sentry_role_delete_users(1:TAlterSentryRoleDeleteUsersRequest request)

  TListSentryRolesResponse list_sentry_roles_by_group(1:TListSentryRolesRequest request)
  TListSentryRolesResponse list_sentry_roles_by_user(1:TListSentryRolesForUserRequest request)

  TListSentryPrivilegesResponse list_sentry_privileges_by_role(1:TListSentryPrivilegesRequest request)

  # For use with ProviderBackend.getPrivileges only
  TListSentryPrivilegesForProviderResponse list_sentry_privileges_for_provider(1:TListSentryPrivilegesForProviderRequest request)

  TDropPrivilegesResponse drop_sentry_privilege(1:TDropPrivilegesRequest request);

  TRenamePrivilegesResponse rename_sentry_privilege(1:TRenamePrivilegesRequest request);

  TListSentryPrivilegesByAuthResponse list_sentry_privileges_by_authorizable(1:TListSentryPrivilegesByAuthRequest request);

  TSentryConfigValueResponse get_sentry_config_value(1:TSentryConfigValueRequest request);

  # export the mapping data in sentry
  TSentryExportMappingDataResponse export_sentry_mapping_data(1:TSentryExportMappingDataRequest request);

  # import the mapping data in sentry
  TSentryImportMappingDataResponse import_sentry_mapping_data(1:TSentryImportMappingDataRequest request);

  # Synchronize between HMS notifications and Sentry
  TSentrySyncIDResponse sentry_sync_notifications(1:TSentrySyncIDRequest request);
}
