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
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.annotations.InterfaceAudience;

@InterfaceAudience.Private
public abstract class SecurityUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);
  public static final String KUDU_TICKETCACHE_PROPERTY = "kudu.krb5ccname";

  /**
   * Map from the names of digest algorithms used in X509 certificates to
   * the appropriate MessageDigest implementation to use for channel-bindings.
   */
  private static final Map<String, String> CERT_DIGEST_TO_MESSAGE_DIGEST =
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
      throw Throwables.propagate(e);
    }
  }

  /**
   * TrustManager implementation which will trust any certificate.
   * TODO(PKI): this needs to change so that it can be configured with
   * the cluster's CA cert.
   */
  static class TrustAnyCert implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] arg0, String arg1)
        throws CertificateException {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] arg0, String arg1)
        throws CertificateException {
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return null;
    }
  }

  /**
   * Create an SSL engine configured to trust any certificate.
   * @return
   * @throws SSLException
   */
  public static SSLEngine createSslEngine() throws SSLException {
    try {
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(null, new TrustManager[] { new TrustAnyCert() }, null);
      return ctx.createSSLEngine();
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, SSLException.class);
      throw Throwables.propagate(e);
    }
  }
}
