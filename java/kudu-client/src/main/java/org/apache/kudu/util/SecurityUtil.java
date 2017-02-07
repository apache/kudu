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

package org.apache.kudu.util;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.annotations.InterfaceAudience;

@InterfaceAudience.Private
public abstract class SecurityUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);
  public static final String KUDU_TICKETCACHE_PROPERTY = "kudu.krb5ccname";

  /**
   * Return the Subject associated with the current thread's AccessController,
   * if that subject has Kerberos credentials. If there is no such subject, or
   * the subject has no Kerberos credentials, logins in a new subject from the
   * currently configured TicketCache.
   */
  public static Subject getSubjectOrLogin() {
    AccessControlContext context = AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    if (subject != null &&
        !subject.getPrincipals(KerberosPrincipal.class).isEmpty()) {
      LOG.debug("Using existing subject with Kerberos credentials: {}",
          subject.toString());
      return subject;
    }
    // If there isn't any current subject with krb5 principals, try to login
    // using the ticket cache.
    Configuration conf = new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        Map<String, String> options = new HashMap<>();

        // TODO: should we offer some kind of "renewal thread" or
        // "reacquire from keytab thread" like Hadoop does?
        options.put("useTicketCache", "true");
        options.put("doNotPrompt", "true");
        options.put("refreshKrb5Config", "true");

        // Allow configuring debug by a system property.
        options.put("debug", Boolean.toString(Boolean.getBoolean("kudu.jaas.debug")));

        // Look for the ticket cache specified in one of the following ways:
        // 1) in a Kudu-specific system property (this is convenient for testing)
        // 2) in the KRB5CCNAME environment variable
        // 3) the Java default (by not setting any value)
        String ticketCache = System.getProperty(KUDU_TICKETCACHE_PROPERTY,
            System.getenv("KRB5CCNAME"));
        if (ticketCache != null) {
          LOG.debug("Using ticketCache: {}", ticketCache);
          options.put("ticketCache", ticketCache);
        }
        options.put("renewTGT", "true");

        return new AppConfigurationEntry[] { new AppConfigurationEntry(
            "com.sun.security.auth.module.Krb5LoginModule",
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
      }
    };
    try {
      LoginContext loginContext = new LoginContext("kudu", new Subject(), null, conf);
      loginContext.login();
      subject = loginContext.getSubject();
      LOG.debug("Logged in as subject: {}", Joiner.on(",").join(subject.getPrincipals()));
      return subject;
    } catch (LoginException e) {
      LOG.debug("Could not login via JAAS. Using no credentials", e);
      return null;
    }
  }

}
