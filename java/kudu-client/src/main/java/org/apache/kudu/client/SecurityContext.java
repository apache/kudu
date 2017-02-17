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

import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.Subject;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.kudu.client.Client.AuthenticationCredentialsPB;
import org.apache.kudu.security.Token.SignedTokenPB;
import org.apache.kudu.security.Token.TokenPB;

/**
 * Class associated with a single AsyncKuduClient which stores security-related
 * infrastructure, credentials, and trusted certificates.
 *
 * Each client has a single instance of this class. This class is threadsafe.
 */
class SecurityContext {
  @GuardedBy("this")
  private SignedTokenPB authnToken;

  private final DelegatedTrustManager trustManager = new DelegatedTrustManager();

  /**
   * SSLContext which trusts only the configured certificate.
   */
  private final SSLContext sslContextWithCert;

  /**
   * SSLContext which trusts any certificate.
   */
  private final SSLContext sslContextTrustAny;

  /**
   * The JAAS Subject that the client's credentials are stored in.
   */
  @Nullable
  private final Subject subject;

  /**
   * The currently trusted CA certs, in DER format.
   */
  private List<ByteString> trustedCertDers = Collections.emptyList();

  public SecurityContext(Subject subject) {
    try {
      this.subject = subject;

      this.sslContextWithCert = SSLContext.getInstance("TLS");
      sslContextWithCert.init(null, new TrustManager[] { trustManager }, null);

      this.sslContextTrustAny = SSLContext.getInstance("TLS");
      sslContextTrustAny.init(null, new TrustManager[] { new TrustAnyCert() }, null);

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Subject getSubject() {
    return subject;
  }

  public synchronized byte[] exportAuthenticationCredentials() {
    if (authnToken == null || !hasTrustedCerts()) {
      return null;
    }

    return AuthenticationCredentialsPB.newBuilder()
        .setAuthnToken(authnToken)
        .addAllCaCertDers(trustedCertDers)
        .build().toByteArray();
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
      if (authnToken != null) {
        checkUserMatches(authnToken, pb.getAuthnToken());
      }
      // TODO(todd): also check that, if there is a JAAS subject, that
      // the subject in the imported authn token matces the Kerberos
      // principal in the JAAS subject. Alternatively, this could
      // completely disable the JAAS authentication path (assumedly if
      // we import a token, we want to _only_ act as the user in that
      // token, and would rather have a connection failure than flip
      // back to GSSAPI transparently.
      trustCertificates(pb.getCaCertDersList());
      authnToken = pb.getAuthnToken();

    } catch (InvalidProtocolBufferException | CertificateException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * @return the current authentication token, or null if we have no valid token
   */
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
      Throwables.propagateIfInstanceOf(e, CertificateException.class);
      throw Throwables.propagate(e);
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
    AtomicReference<X509TrustManager> delegate = new AtomicReference<>();

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
