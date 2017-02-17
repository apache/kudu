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
import com.google.protobuf.ByteString;

import org.apache.kudu.security.Token.SignedTokenPB;

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
  public boolean hasTrustedCerts() {
    return trustManager.delegate.get() != null;
  }

  /**
   * Mark the given CA cert (provided in DER form) as the trusted CA cert for the
   * client. Replaces any previously trusted cert.
   * @throws CertificateException if the cert was invalid
   */
  public void trustCertificate(ByteString certDer) throws CertificateException {
    CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
    X509Certificate cert = (X509Certificate)certFactory.generateCertificate(
        certDer.newInput());

    // This is implemented by making a new TrustManager and swapping it out under
    // our delegating trust manager. It might seem more straight-forward to instead
    // just keep one keystore around and load new certs into it, but apparently the
    // TrustManager loads the certs from the KeyStore upon creation, so adding new
    // ones to an existing KeyStore doesn't have any effect.
    try {
      KeyStore certKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      certKeyStore.load(null);
      certKeyStore.setCertificateEntry("my-ca", cert);

      TrustManagerFactory tmf = TrustManagerFactory.getInstance(
          TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(certKeyStore);
      TrustManager[] managers = tmf.getTrustManagers();
      if (managers.length != 1) {
        throw new RuntimeException("TrustManagerFactory generated multiple TrustManagers");
      }
      trustManager.delegate.set((X509TrustManager) managers[0]);
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, CertificateException.class);
      throw Throwables.propagate(e);
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
