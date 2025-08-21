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

package org.apache.kudu.subprocess.ranger.authorization;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ranger.Ranger;
import org.apache.kudu.ranger.Ranger.RangerRequestListPB;
import org.apache.kudu.ranger.Ranger.RangerRequestPB;
import org.apache.kudu.subprocess.KuduSubprocessException;

public class RangerKuduAuthorizer {
  private static final Logger LOG = LoggerFactory.getLogger(RangerKuduAuthorizer.class);
  // The following properties need to match the Kudu service def in Ranger
  // (https://github.com/apache/ranger/blob/master/agents-common/src/main/resources/service-defs/ranger-servicedef-kudu.json).
  private static final String APP_ID = "kudu";
  private static final String RANGER_DB_RESOURCE_NAME = "database";
  private static final String RANGER_TABLE_RESOURCE_NAME = "table";
  private static final String RANGER_COLUMN_RESOURCE_NAME = "column";
  private static final String SERVICE_TYPE = "kudu";

  // The Ranger Kudu plugin. This field is not final as it is used in the
  // mock test.
  @InterfaceAudience.LimitedPrivate("Test")
  RangerBasePlugin plugin;

  public RangerKuduAuthorizer() {
    plugin = new RangerBasePlugin(SERVICE_TYPE, APP_ID);
    plugin.setResultProcessor(new RangerDefaultAuditHandler());
  }

  /**
   * Initializes the Ranger Kudu plugin, which has to be called explicitly
   * before doing any authorizations.
   *
   * @param servicePrincipal the principal name for Kudu to load from the keytab file
   * @param keytab the path to the Kudu keytab file
   */
  public void init(String servicePrincipal, String keytab) {
    // Determine if Kerberos is enabled in the Hadoop configuration. Kerberos should
    // also be enabled in the Kudu master.
    if (UserGroupInformation.isSecurityEnabled()) {
      if (servicePrincipal.isEmpty() || keytab.isEmpty()) {
        throw new KuduSubprocessException("Kudu principal and Keytab file must be " +
                                          "provided when Kerberos is enabled in Ranger");
      }
      // When Kerberos is enabled, login with the Kudu principal and keytab
      // before initializing the Ranger plugin.
      try {
        LOG.debug("Login with Kudu principal: {}, and keytab: {}", servicePrincipal, keytab);
        UserGroupInformation.loginUserFromKeytab(servicePrincipal, keytab);
      } catch (IOException e) {
        throw new KuduSubprocessException("Failed to login with Kudu principal/keytab", e);
      }
    }
    plugin.init();
    LOG.info("Finished Ranger Kudu plugin initialization");
  }

  /**
   *  Authorizes a given <code>RangerRequestListPB</code> in Ranger and returns
   *  a list of <code>RangerAccessResult</code> which contains the authorization
   *  decisions. Note that the order of results is determined by the order of
   *  requests.
   *
   * @param requests a RangerRequestListPB
   * @return a list of RangerAccessResult
   */
  @VisibleForTesting
  public Ranger.RangerResponseListPB.Builder authorize(RangerRequestListPB requests) {
    if (!requests.hasUser() || requests.getUser().isEmpty()) {
      Ranger.RangerResponseListPB.Builder rangerResponseListPB = Ranger.RangerResponseListPB
          .newBuilder();
      List<RangerRequestPB> requestsList = requests.getRequestsList();
      for (int i = 0, requestsListSize = requestsList.size(); i < requestsListSize; i++) {
        Ranger.RangerResponsePB response = Ranger.RangerResponsePB.newBuilder()
            .setAllowed(false)
            .build();
        rangerResponseListPB.addResponses(response);
      }
      return rangerResponseListPB;
    }
    return authorizeRequests(requests);
  }

  /**
   * Refreshes the policies cached in the authorization provider on a best-effort basis. It
   * doesn't guarantee invalidating the cache or that the latest policies could be pulled from
   * the server and doesn't throw exceptions even if the server couldn't be reached.
   *
   * TODO(abukor): Revisit if RANGER-2906 is fixed.
   */
  public void refreshPolicies() {
    LOG.debug("Refreshing policies...");
    plugin.refreshPoliciesAndTags();
    LOG.debug("Refreshing policies... DONE");
  }

  /**
   * Creates a Ranger access request for the specified user who performs
   * the given action on the resource.
   *
   * @param action action to be authorized on the resource. Note that when it
   *               is null, Ranger will match to any valid actions
   * @param user user who is performing the action
   * @param groups the set of groups the user belongs to
   * @param db the database name the action is to be performed on
   * @param table the table name the action is to be performed on
   * @param col the column name the action is to be performed on
   * @return the ranger access request
   */
  private static RangerAccessRequestImpl createRequest(
      @Nullable String action, String user,
      @Nullable Set<String> groups, @Nullable String db,
      @Nullable String table, @Nullable String col) {
    final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
    resource.setValue(RANGER_DB_RESOURCE_NAME, db);
    resource.setValue(RANGER_TABLE_RESOURCE_NAME, table);
    resource.setValue(RANGER_COLUMN_RESOURCE_NAME, col);

    final RangerAccessRequestImpl request = new RangerAccessRequestImpl();
    request.setResource(resource);
    request.setAccessType(action);
    // Add action as it is used for auditing in Ranger.
    request.setAction(action);
    request.setUser(user);
    request.setUserGroups(groups);
    return request;
  }

  /**
   * Creates a <code>RangerResponseListPB</code> for the given
   * <code>RangerRequestListPB</code>.
   *
   * @param requests the given RangerRequestListPB
   * @return a list of RangerAccessRequest
   */
  private Ranger.RangerResponseListPB.Builder authorizeRequests(RangerRequestListPB requests) {
    Ranger.RangerResponseListPB.Builder rangerResponseList = Ranger.RangerResponseListPB
        .newBuilder();
    Preconditions.checkArgument(requests.hasUser());
    Preconditions.checkArgument(!requests.getUser().isEmpty());
    final String user = requests.getUser();
    Set<String> groups = getUserGroups(user);
    for (RangerRequestPB request : requests.getRequestsList()) {
      // Action should be lower case to match the Kudu service def in Ranger.
      String action = request.getAction().name().toLowerCase(Locale.ENGLISH);
      String db = request.hasDatabase() ? request.getDatabase() : null;
      String table = request.hasTable() ? request.getTable() : null;
      String column = request.hasColumn() ? request.getColumn() : null;
      boolean requiresAdmin = request.hasRequiresDelegateAdmin() &&
          request.getRequiresDelegateAdmin();
      boolean isOwner = request.hasIsOwner() && request.getIsOwner();
      RangerAccessRequest rangerAccessRequest = createRequest(action, user, groups, db, table,
                                                              column);
      RangerAccessResult rangerAccessResult = plugin.isAccessAllowed(rangerAccessRequest);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("RangerAccessRequest [%s] receives result [%s]",
            rangerAccessResult.getAccessRequest().toString(), rangerAccessResult.toString()));
      }
      if (!rangerAccessResult.getIsAllowed() && isOwner) {
        rangerAccessRequest = createRequest(action, RangerPolicyEngine.RESOURCE_OWNER, groups,
                                            db, table, column);
        rangerAccessResult = plugin.isAccessAllowed(rangerAccessRequest);
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("RangerAccessRequest [%s] receives result [%s]",
              rangerAccessResult.getAccessRequest().toString(), rangerAccessResult.toString()));
        }
      }
      if (rangerAccessResult.getIsAllowed() && requiresAdmin) {
        rangerAccessRequest = createRequest(RangerPolicyEngine.ADMIN_ACCESS, user, groups, db,
                                            table, column);
        rangerAccessResult = plugin.isAccessAllowed(rangerAccessRequest);
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("RangerAccessRequest [%s] receives result [%s]",
              rangerAccessResult.getAccessRequest().toString(), rangerAccessResult.toString()));
        }
        if (!rangerAccessResult.getIsAllowed() && isOwner) {
          rangerAccessRequest = createRequest(RangerPolicyEngine.ADMIN_ACCESS,
                                              RangerPolicyEngine.RESOURCE_OWNER,
                                              groups, db, table, column);
          rangerAccessResult = plugin.isAccessAllowed(rangerAccessRequest);
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("RangerAccessRequest [%s] receives result [%s]",
                rangerAccessResult.getAccessRequest().toString(), rangerAccessResult.toString()));
          }
        }
      }

      Ranger.RangerResponsePB rangerResponsePB = Ranger.RangerResponsePB.newBuilder()
          .setAllowed(rangerAccessResult.getIsAllowed())
          .build();

      rangerResponseList.addResponses(rangerResponsePB);
    }
    return rangerResponseList;
  }

  /**
   * Gets the user group mapping from Hadoop. The groups of a user is determined by a
   * group mapping service provider. See more detail at
   * https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/GroupsMapping.html.
   *
   * @param user the user name
   * @return the set of groups the user belongs to
   */
  private static Set<String> getUserGroups(String user) {
    Preconditions.checkNotNull(user);
    Preconditions.checkArgument(!user.isEmpty());
    UserGroupInformation ugi;
    ugi = UserGroupInformation.createRemoteUser(user);
    return new HashSet<>(ugi.getGroups());
  }
}
