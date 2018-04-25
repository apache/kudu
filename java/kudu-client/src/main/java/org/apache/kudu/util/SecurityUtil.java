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

import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public abstract class SecurityUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);
  public static final String KUDU_TICKETCACHE_PROPERTY = "kudu.krb5ccname";

  /**
   * Map from the names of digest algorithms used in X509 certificates to
   * the appropriate MessageDigest implementation to use for channel-bindings.
   */
  private static final ImmutableMap<String, String> CERT_DIGEST_TO_MESSAGE_DIGEST =
      ImmutableMap.<String, String>builder()
      // RFC 5929: if the certificate's signatureAlgorithm uses a single hash
      // function, and that hash function is either MD5 [RFC1321] or SHA-1
      // [RFC3174], then use SHA-256 [FIPS-180-3];
      .put("MD5", "SHA-256")
      .put("SHA1", "SHA-256")
      // For other algorithms, use the provided hash function.
      .put("SHA224", "SHA-224")
      .put("SHA256", "SHA-256")
      .put("SHA384", "SHA-384")
      .put("SHA512", "SHA-512")
      // The above list is exhaustive as of JDK8's implementation of
      // SignatureAndHashAlgorithm.
      .build();

  /**
   * If we have Kerberos credentials that are within this specified window
   * of expiration, then refresh them.
   */
  private static final long REFRESH_BEFORE_EXPIRATION_SECS = 10;

  /**
   * Return the Subject associated with the current thread's AccessController,
   * if that subject has Kerberos credentials. If there is no such subject, or
   * the subject has no Kerberos credentials, a new subject is logged in from
   * the currently configured TicketCache.
   */
  @Nullable
  public static Subject getSubjectFromTicketCacheOrNull() {
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
      Subject subject = loginContext.getSubject();
      LOG.debug("Logged in as subject: {}", Joiner.on(",").join(subject.getPrincipals()));
      return subject;
    } catch (LoginException e) {
      LOG.debug("Could not login via JAAS. Using no credentials: " + e.getMessage(),
          LOG.isTraceEnabled() ? e : null);
      return null;
    }
  }

  /**
   * Compute the "tls-server-endpoint" channel binding data for the given X509
   * certificate. The algorithm is specified in RFC 5929.
   *
   * @return the expected channel bindings for the certificate
   * @throws RuntimeException if the certificate is not an X509 cert, or if
   * it uses a signature type for which we cannot compute channel bindings
   */
  public static byte[] getEndpointChannelBindings(Certificate cert) {
    Preconditions.checkArgument(cert instanceof X509Certificate,
        "can only handle X509 certs");
    X509Certificate x509 = (X509Certificate)cert;
    String sigAlg = x509.getSigAlgName();

    // The signature algorithm name is a string like 'SHA256withRSA'.
    // There's no API available to actually find just the digest algorithm,
    // so we resort to some hackery.
    String[] components = sigAlg.split("with");
    String digestAlg = CERT_DIGEST_TO_MESSAGE_DIGEST.get(components[0].toUpperCase());
    if (digestAlg == null) {
      // RFC 5929: if the certificate's signatureAlgorithm uses no hash functions or
      // uses multiple hash functions, then this channel binding type's channel
      // bindings are undefined at this time (updates to is channel binding type may
      // occur to address this issue if it ever arises).
      throw new RuntimeException("cert uses unknown signature algorithm: " + sigAlg);
    }
    try {
      return MessageDigest.getInstance(digestAlg).digest(cert.getEncoded());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return true if 'subject' contains a Kerberos TGT that is about to expire, or
   * if it contains no TGT at all.
   */
  public static boolean needsRefresh(Subject subject) {
    long deadline = System.currentTimeMillis() + REFRESH_BEFORE_EXPIRATION_SECS * 1000;
    return tgtExpiresBefore(subject, deadline);
  }

  /**
   * @return true if 'subject' contains a Kerberos TGT that is expired, or if it contains
   * no TGT at all.
   */
  public static boolean isTgtExpired(Subject subject) {
    return tgtExpiresBefore(subject, System.currentTimeMillis());
  }

  private static boolean tgtExpiresBefore(Subject subject, long deadlineMillis) {
    KerberosTicket tgt = findTgt(subject);
    if (tgt != null) {
      return tgt.getEndTime().getTime() < deadlineMillis;
    }
    // We didn't find any TGT. This likely means that it expired and got
    // removed during a connection attempt. So, we need to get a new one.
    return true;
  }

  private static KerberosTicket findTgt(Subject subject) {
    Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
    // tickets is a Collections.synchronizedSet() wrapper, so we need to synchronize
    // on it to iterate it.
    synchronized (tickets) {
      for (KerberosTicket ticket : tickets) {
        if (SecurityUtil.isTGSPrincipal(ticket.getServer())) {
          return ticket;
        }
      }
    }
    return null;
  }

  /**
   * @return true if 'principal' matches the expected pattern for a TGT
   */
  private static boolean isTGSPrincipal(KerberosPrincipal principal) {
    // When a principal foo@BAR authenticates to realm BAR, it will get a service
    // ticket with the service principal 'krbtgt/BAR@BAR'. Note that this is the
    // case even when the credentials will be used to authenticate to a remote
    // realm using cross-realm trust.
    //
    // For example, if the user alice@AD.CORP is connecting to a Kudu service
    // kudu/host@CLUSTER.LOCAL, the ticket cache will contain the following
    // tickets:
    //
    //   krbtgt/AD.CORP@AD.CORP
    //   krbtgt/CLUSTER.LOCAL@AD.CORP   (cross-realm trust ticket)
    //   kudu/host@CLUSTER.LOCAL        (service in remote realm)
    //
    // Here we are simply trying to identify the first of those tickets.
    return principal != null && principal.getName().equals(
        "krbtgt/" + principal.getRealm() + "@" + principal.getRealm());
  }

  /**
   * @return the KerberosPrincipal object associated with the given Subject.
   * If there is no Principal, returns null. If there is more than one principal
   * (not expected), logs a warning and also returns null.
   */
  public static KerberosPrincipal getKerberosPrincipalOrNull(Subject newSubject) {
    Set<KerberosPrincipal> principals = newSubject.getPrincipals(KerberosPrincipal.class);
    if (principals.size() > 1) {
      LOG.warn("JAAS Subject unexpectedly includes more than one principal: {}",
          Joiner.on(", ").join(principals));
      return null;
    } else if (principals.isEmpty()) {
      return null;
    }

    return principals.iterator().next();
  }
}