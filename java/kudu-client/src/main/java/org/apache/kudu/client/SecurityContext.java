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

package org.apache.kudu.client;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.KeyStore;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.kudu.client.Client.AuthenticationCredentialsPB;
import org.apache.kudu.security.Token.SignedTokenPB;
import org.apache.kudu.security.Token.TokenPB;
import org.apache.kudu.util.Pair;
import org.apache.kudu.util.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class associated with a single AsyncKuduClient which stores security-related
 * infrastructure, credentials, and trusted certificates.
 *
 * Each client has a single instance of this class. This class is threadsafe.
 */
class SecurityContext {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);

  private static final long REFRESH_RATE_LIMIT_SECS = 10;

  @GuardedBy("this")
  @Nullable
  private SignedTokenPB authnToken;

  @GuardedBy("this")
  private String realUser;

  private final DelegatedTrustManager trustManager = new DelegatedTrustManager();

  /**
   * SSLContext which trusts only the configured certificate.
   */
  private final SSLContext sslContextWithCert;

  /**
   * SSLContext which trusts any certificate.
   */
  private final SSLContext sslContextTrustAny;

  private final Object subjectLock = new Object();

  /**
   * The JAAS Subject that the client's credentials are stored in.
   */
  @Nullable
  @GuardedBy("subjectLock")
  private Subject subject;

  private enum SubjectType {
    /**
     * The Subject was created when this class was instantiated.
     */
    CREATED,
    /**
     * A Subject with appropriate credentials was provided by the caller who
     * instantiated this class.
     */
    PROVIDED,
    /**
     * We have no Subject at all (i.e we could not login on our own, and the
     * caller did not provide a Subject with appropriate credentials).
     */
    NONE
  }

  @Nonnull
  private final SubjectType subjectType;

  /**
   * The currently trusted CA certs, in DER format.
   */
  @VisibleForTesting
  @GuardedBy("this")
  List<ByteString> trustedCertDers = Collections.emptyList();

  @GuardedBy("subjectLock")
  private long nextAllowedRefreshNanotime = 0;

  @GuardedBy("subjectLock")
  private boolean loggedRefreshFailure = false;

  SecurityContext() {
    try {
      Pair<SubjectType, Subject> p = setupSubject();
      this.subjectType = p.getFirst();
      this.subject = p.getSecond();

      this.realUser = System.getProperty("user.name");

      this.sslContextWithCert = SSLContext.getInstance("TLS");
      sslContextWithCert.init(null, new TrustManager[] { trustManager }, null);

      this.sslContextTrustAny = SSLContext.getInstance("TLS");
      sslContextTrustAny.init(null, new TrustManager[] { new TrustAnyCert() }, null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Pair<SubjectType, Subject> setupSubject() {
    AccessControlContext context = AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    if (subject != null) {
      if (!subject.getPrincipals(KerberosPrincipal.class).isEmpty()) {
        LOG.debug("Using caller-provided subject with Kerberos principal {}. " +
            "Caller is responsible for refreshing credentials.",
            SecurityUtil.getKerberosPrincipalOrNull(subject));
        return new Pair<>(SubjectType.PROVIDED, subject);
      }
      LOG.debug("Caller-provided subject {} does not have any Kerberos credentials. " +
          "Ignoring it.", subject.toString());
    }

    subject = SecurityUtil.getSubjectFromTicketCacheOrNull();
    if (subject != null) {
      return new Pair<>(SubjectType.CREATED, subject);
    }
    // If we weren't able to login from a ticket cache when we create the client,
    // we shouldn't later pick one up.
    return new Pair<>(SubjectType.NONE, null);
  }

  /**
   * Check if the Subject associated with this SecurityContext needs to be refreshed,
   * and if so, do so. If there is no associated subject this is a no-op.
   */
  public void refreshSubject() {
    if (subjectType == SubjectType.NONE) {
      return;
    }
    synchronized (subjectLock) {
      Subject localSubject = subject;

      boolean needed = SecurityUtil.needsRefresh(localSubject);
      if (!needed) {
        // If we don't need to refresh, but we previously logged a warning
        // about a failure to refresh, then someone must have externally
        // refreshed the Subject.
        if (loggedRefreshFailure) {
          LOG.info("Credentials appear to have been refreshed externally, subject={}", subject);
          loggedRefreshFailure = false;
        }
        return;
      }

      // Our current credentials are stale and need a refresh.

      if (subjectType == SubjectType.PROVIDED) {
        // In the case that the user provided the subject, we don't attempt to
        // muck with the tickets inside it. Instead, just log a warning
        // if we haven't already.
        if (!loggedRefreshFailure) {
          LOG.warn("Caller-provided Subject has a Kerberos ticket that is about to expire. " +
                   "Kudu expects the application to renew or re-acquire its own tickets " +
                   "before expiration.");
          loggedRefreshFailure = true;
        }
        return;
      }

      // Don't attempt to refresh if we recently attempted to and failed. This
      // prevents flooding the KDC, etc.
      long now = System.nanoTime();
      // If we recently failed to refresh, don't retry.
      if (now < nextAllowedRefreshNanotime) {
        return;
      }

      LOG.debug("Refreshing Kerberos credentials...");
      Subject newSubject;
      try {
        newSubject = Subject.doAs(new Subject(), new PrivilegedExceptionAction<Subject>() {
          @Override
          public Subject run() {
            return SecurityUtil.getSubjectFromTicketCacheOrNull();
          }
        });
      } catch (PrivilegedActionException e) {
        throw new RuntimeException(e.getCause());
      }
      if (newSubject == null || SecurityUtil.getKerberosPrincipalOrNull(newSubject) == null) {
        LOG.warn("Tried to refresh Kerberos credentials but was unable to re-login from ticket cache");
        loggedRefreshFailure = true;
        nextAllowedRefreshNanotime = now + TimeUnit.SECONDS.toNanos(REFRESH_RATE_LIMIT_SECS);
        return;
      }
      // It's possible that the ticket cache ended up with a different principal.
      // If we accepted this new subject, that would cause us to switch principals
      // in the context of a single Kudu client, or potentially have a different
      // principal in use on different connections (eg one principal talking to one
      // master and another principal to another). This would be very hard to diagnose
      // so let's just refuse the re-login attempt if the principal switched.
      KerberosPrincipal oldPrincipal = SecurityUtil.getKerberosPrincipalOrNull(localSubject);
      KerberosPrincipal principal = SecurityUtil.getKerberosPrincipalOrNull(newSubject);
      if (!oldPrincipal.equals(principal)) {
        LOG.error("Attempted to refresh Kerberos credentials from ticket cache but found that " +
            "the new Kerberos principal {} did not match the original principal {}. Ignoring.",
            principal, oldPrincipal);
        loggedRefreshFailure = true;
        nextAllowedRefreshNanotime = now + TimeUnit.SECONDS.toNanos(REFRESH_RATE_LIMIT_SECS);
        return;
      }

      loggedRefreshFailure = false;
      this.subject = newSubject;
      LOG.info("Successfully refreshed Kerberos credentials from ticket cache");
    }
  }

  @Nullable
  public Subject getSubject() {
    synchronized (subjectLock) {
      return subject;
    }
  }

  public synchronized String getRealUser() {
    return realUser;
  }

  @Nullable
  public synchronized byte[] exportAuthenticationCredentials() {
    AuthenticationCredentialsPB.Builder pb = AuthenticationCredentialsPB.newBuilder();
    pb.setRealUser(realUser);
    if (authnToken != null) {
      pb.setAuthnToken(authnToken);
    }
    pb.addAllCaCertDers(trustedCertDers);
    return pb.build().toByteArray();
  }

  private static String getUserFromToken(SignedTokenPB token)
      throws InvalidProtocolBufferException {
    TokenPB pb = TokenPB.parseFrom(token.getTokenData());
    return pb.getAuthn().getUsername();
  }

  private static void checkUserMatches(SignedTokenPB oldToken, SignedTokenPB newToken)
      throws InvalidProtocolBufferException {
    String oldUser = getUserFromToken(oldToken);
    String newUser = getUserFromToken(newToken);

    if (!oldUser.equals(newUser)) {
      throw new IllegalArgumentException(String.format(
          "cannot import authentication data from a different user: old='%s', new='%s'",
          oldUser, newUser));
    }
  }

  public synchronized void importAuthenticationCredentials(byte[] authnData) {
    try {
      AuthenticationCredentialsPB pb = AuthenticationCredentialsPB.parseFrom(authnData);
      if (pb.hasAuthnToken() && authnToken != null) {
        // TODO(todd): also check that, if there is a JAAS subject, that
        // the subject in the imported authn token matches the Kerberos
        // principal in the JAAS subject. Alternatively, this could
        // completely disable the JAAS authentication path (assumedly if
        // we import a token, we want to _only_ act as the user in that
        // token, and would rather have a connection failure than flip
        // back to GSSAPI transparently).
        checkUserMatches(authnToken, pb.getAuthnToken());
      }

      authnToken = pb.getAuthnToken();
      trustCertificates(pb.getCaCertDersList());

      if (pb.hasRealUser()) {
        realUser = pb.getRealUser();
      }
    } catch (InvalidProtocolBufferException | CertificateException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * @return the current authentication token, or null if we have no valid token
   */
  @Nullable
  public synchronized SignedTokenPB getAuthenticationToken() {
    return authnToken;
  }

  /**
   * Set the token that we will use to authenticate to servers. Replaces any
   * prior token.
   */
  public synchronized void setAuthenticationToken(SignedTokenPB token) {
    authnToken = token;
  }

  /**
   * Create an SSLEngine which will trust all certificates without verification.
   */
  public SSLEngine createSSLEngineTrustAll() {
    return sslContextTrustAny.createSSLEngine();
  }

  /**
   * Create an SSLEngine which will trust only certificates that have a valid chain
   * of trust.
   */
  public SSLEngine createSSLEngine() {
    return sslContextWithCert.createSSLEngine();
  }

  /**
   * @return true if any cert has been marked as trusted
   */
  public synchronized boolean hasTrustedCerts() {
    return !trustedCertDers.isEmpty();
  }

  /**
   * Create a trust manager which will trust all of the given CA certs.
   */
  private static X509TrustManager createTrustManagerForCerts(Iterable<ByteString> certDers)
      throws CertificateException {
    CertificateFactory certFactory = CertificateFactory.getInstance("X.509");

    List<X509Certificate> certs = Lists.newArrayList();
    for (ByteString certDer : certDers) {
      certs.add((X509Certificate)certFactory.generateCertificate(
          certDer.newInput()));
    }

    // This is implemented by making a new TrustManager and swapping it out under
    // our delegating trust manager. It might seem more straight-forward to instead
    // just keep one keystore around and load new certs into it, but apparently the
    // TrustManager loads the certs from the KeyStore upon creation, so adding new
    // ones to an existing KeyStore doesn't have any effect.
    try {
      KeyStore certKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      certKeyStore.load(null);
      int i = 0;
      for (X509Certificate cert : certs) {
        certKeyStore.setCertificateEntry(String.format("cert-%d",  i++), cert);
      }

      TrustManagerFactory tmf = TrustManagerFactory.getInstance(
          TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(certKeyStore);
      TrustManager[] managers = tmf.getTrustManagers();
      if (managers.length != 1) {
        throw new RuntimeException("TrustManagerFactory generated multiple TrustManagers");
      }
      return (X509TrustManager) managers[0];
    } catch (Exception e) {
      Throwables.throwIfInstanceOf(e, CertificateException.class);
      throw new RuntimeException(e);
    }
  }

  /**
   * Mark the given CA cert (provided in DER form) as the trusted CA cert for the
   * client. Replaces any previously trusted cert.
   * @throws CertificateException if the cert was invalid
   */
  public void trustCertificates(List<ByteString> certDers) throws CertificateException {
    X509TrustManager tm = createTrustManagerForCerts(certDers);
    synchronized (this) {
      trustManager.delegate.set(tm);
      trustedCertDers = ImmutableList.copyOf(certDers);
    }
  }

  /**
   * TrustManager implementation which will trust any certificate.
   */
  private static class TrustAnyCert implements X509TrustManager {
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
   * Trust manager that delegates to an underlying trust manager which
   * can be swapped out atomically.
   */
  private static class DelegatedTrustManager implements X509TrustManager {
    final AtomicReference<X509TrustManager> delegate = new AtomicReference<>();

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      delegate.get().checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      delegate.get().checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return delegate.get().getAcceptedIssuers();
    }
  }

}
