package com.gizmodata.chargewire.jdbc.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

/** Builds the {@link SSLContext} for a connection's TLS options. */
final class TlsContexts {
  private static final Pattern PEM =
      Pattern.compile("-----BEGIN ([A-Z0-9 ]+)-----([^-]*)-----END \\1-----");
  private static final char[] NO_PASSWORD = new char[0];

  private TlsContexts() {}

  static SSLContext create(ConnectionConfig config) throws GeneralSecurityException, IOException {
    TrustManager[] trustManagers =
        config.disableCertificateVerification
            ? new TrustManager[] {new AcceptingTrustManager()}
            : trustManagers(config);
    KeyManager[] keyManagers = null;
    if (config.clientCertificate != null || config.clientKey != null) {
      if (config.clientCertificate == null || config.clientKey == null) {
        throw new GeneralSecurityException("mutual TLS needs both clientCertificate and clientKey");
      }
      keyManagers = keyManagers(config.clientCertificate, config.clientKey);
    }
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(keyManagers, trustManagers, null);
    return context;
  }

  private static TrustManager[] trustManagers(ConnectionConfig config)
      throws GeneralSecurityException, IOException {
    KeyStore trusted = KeyStore.getInstance("PKCS12");
    trusted.load(null, null);
    List<X509Certificate> certificates = new ArrayList<>();
    if (config.useSystemTrustStore) {
      certificates.addAll(jdkTrustedCertificates());
      certificates.addAll(operatingSystemCertificates());
    }
    if (config.tlsRootCerts != null) {
      certificates.addAll(readCertificates(config.tlsRootCerts));
    }
    if (config.trustStore != null) {
      char[] password =
          config.trustStorePassword == null ? null : config.trustStorePassword.toCharArray();
      KeyStore store = KeyStore.getInstance(config.trustStore.toFile(), password);
      for (String alias : Collections.list(store.aliases())) {
        Certificate certificate = store.getCertificate(alias);
        if (certificate instanceof X509Certificate) {
          certificates.add((X509Certificate) certificate);
        }
      }
    }
    if (certificates.isEmpty()) {
      throw new GeneralSecurityException(
          "no trusted certificates: set tlsRootCerts or trustStore, or useSystemTrustStore=true");
    }
    int index = 0;
    for (X509Certificate certificate : certificates) {
      trusted.setCertificateEntry("ca-" + index++, certificate);
    }
    TrustManagerFactory factory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    factory.init(trusted);
    return factory.getTrustManagers();
  }

  private static List<X509Certificate> jdkTrustedCertificates() throws GeneralSecurityException {
    TrustManagerFactory factory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    factory.init((KeyStore) null);
    List<X509Certificate> certificates = new ArrayList<>();
    for (TrustManager manager : factory.getTrustManagers()) {
      if (manager instanceof X509TrustManager) {
        Collections.addAll(certificates, ((X509TrustManager) manager).getAcceptedIssuers());
      }
    }
    return certificates;
  }

  /** The macOS keychain's or Windows' trusted roots, where the JDK can read them. */
  private static List<X509Certificate> operatingSystemCertificates() {
    String os = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT);
    List<String> storeTypes =
        os.contains("mac")
            ? List.of("KeychainStore-ROOT", "KeychainStore")
            : os.contains("win") ? List.of("Windows-ROOT") : List.of();
    List<X509Certificate> certificates = new ArrayList<>();
    for (String type : storeTypes) {
      try {
        KeyStore store = KeyStore.getInstance(type);
        store.load(null, null);
        for (String alias : Collections.list(store.aliases())) {
          Certificate certificate = store.getCertificate(alias);
          if (certificate instanceof X509Certificate) {
            certificates.add((X509Certificate) certificate);
          }
        }
      } catch (GeneralSecurityException | IOException e) {
        // This JDK cannot read that store; the JDK's own roots still apply.
      }
    }
    return certificates;
  }

  private static List<X509Certificate> readCertificates(Path file)
      throws GeneralSecurityException, IOException {
    CertificateFactory factory = CertificateFactory.getInstance("X.509");
    try (InputStream in = Files.newInputStream(file)) {
      Collection<? extends Certificate> read = factory.generateCertificates(in);
      List<X509Certificate> certificates = new ArrayList<>();
      for (Certificate certificate : read) {
        certificates.add((X509Certificate) certificate);
      }
      if (certificates.isEmpty()) {
        throw new GeneralSecurityException("no certificates in " + file);
      }
      return certificates;
    }
  }

  private static KeyManager[] keyManagers(Path certificateFile, Path keyFile)
      throws GeneralSecurityException, IOException {
    List<X509Certificate> chain = readCertificates(certificateFile);
    PrivateKey key = readPrivateKey(keyFile);
    KeyStore store = KeyStore.getInstance("PKCS12");
    store.load(null, null);
    store.setKeyEntry("client", key, NO_PASSWORD, chain.toArray(new Certificate[0]));
    KeyManagerFactory factory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    factory.init(store, NO_PASSWORD);
    return factory.getKeyManagers();
  }

  /** Reads a PEM private key: PKCS#8, PKCS#1 (RSA) or SEC1 (EC), unencrypted. */
  static PrivateKey readPrivateKey(Path file) throws GeneralSecurityException, IOException {
    String text = Files.readString(file, StandardCharsets.US_ASCII);
    Matcher matcher = PEM.matcher(text);
    while (matcher.find()) {
      String label = matcher.group(1);
      byte[] der = Base64.getMimeDecoder().decode(matcher.group(2));
      switch (label) {
        case "PRIVATE KEY":
          return pkcs8(der);
        case "RSA PRIVATE KEY":
          return pkcs8(Der.pkcs8FromPkcs1(der));
        case "EC PRIVATE KEY":
          return pkcs8(Der.pkcs8FromSec1(der));
        case "ENCRYPTED PRIVATE KEY":
          throw new GeneralSecurityException(
              "encrypted private keys are not supported; decrypt it with "
                  + "'openssl pkcs8 -topk8 -nocrypt -in key.pem'");
        default:
          break;
      }
    }
    throw new GeneralSecurityException("no PEM private key in " + file);
  }

  private static PrivateKey pkcs8(byte[] der) throws GeneralSecurityException {
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
    for (String algorithm : List.of("RSA", "EC", "EdDSA", "RSASSA-PSS")) {
      try {
        return KeyFactory.getInstance(algorithm).generatePrivate(spec);
      } catch (InvalidKeySpecException e) {
        // Try the next algorithm.
      }
    }
    throw new GeneralSecurityException("unsupported private key algorithm");
  }

  /** Minimal DER encoding, enough to wrap PKCS#1 and SEC1 keys as PKCS#8. */
  static final class Der {
    private static final byte[] RSA_ALGORITHM = {
      0x30,
      0x0d,
      0x06,
      0x09,
      0x2a,
      (byte) 0x86,
      0x48,
      (byte) 0x86,
      (byte) 0xf7,
      0x0d,
      0x01,
      0x01,
      0x01,
      0x05,
      0x00
    };
    private static final byte[] EC_PUBLIC_KEY_OID = {
      0x06, 0x07, 0x2a, (byte) 0x86, 0x48, (byte) 0xce, 0x3d, 0x02, 0x01
    };
    private static final byte[] VERSION_0 = {0x02, 0x01, 0x00};

    private Der() {}

    static byte[] pkcs8FromPkcs1(byte[] pkcs1) {
      return sequence(VERSION_0, RSA_ALGORITHM, tlv(0x04, pkcs1));
    }

    /** SEC1 ECPrivateKey carries its curve in [0]; PKCS#8 wants it in the algorithm. */
    static byte[] pkcs8FromSec1(byte[] sec1) throws GeneralSecurityException {
      int[] position = {0};
      expect(sec1, position, 0x30);
      readLength(sec1, position);
      skip(sec1, position, 0x02);
      skip(sec1, position, 0x04);
      if (position[0] >= sec1.length || (sec1[position[0]] & 0xff) != 0xa0) {
        throw new GeneralSecurityException("EC private key does not name its curve");
      }
      position[0]++;
      int length = readLength(sec1, position);
      byte[] curve = new byte[length];
      System.arraycopy(sec1, position[0], curve, 0, length);
      return sequence(VERSION_0, sequence(EC_PUBLIC_KEY_OID, curve), tlv(0x04, sec1));
    }

    private static void expect(byte[] der, int[] position, int tag)
        throws GeneralSecurityException {
      if (position[0] >= der.length || (der[position[0]] & 0xff) != tag) {
        throw new GeneralSecurityException("malformed key: expected DER tag " + tag);
      }
      position[0]++;
    }

    private static void skip(byte[] der, int[] position, int tag) throws GeneralSecurityException {
      expect(der, position, tag);
      int length = readLength(der, position);
      position[0] += length;
    }

    private static int readLength(byte[] der, int[] position) throws GeneralSecurityException {
      if (position[0] >= der.length) {
        throw new GeneralSecurityException("malformed key: truncated");
      }
      int first = der[position[0]++] & 0xff;
      if (first < 0x80) {
        return first;
      }
      int bytes = first & 0x7f;
      if (bytes > 3 || position[0] + bytes > der.length) {
        throw new GeneralSecurityException("malformed key: bad length");
      }
      int length = 0;
      for (int i = 0; i < bytes; i++) {
        length = (length << 8) | (der[position[0]++] & 0xff);
      }
      return length;
    }

    private static byte[] sequence(byte[]... parts) {
      ByteArrayOutputStream body = new ByteArrayOutputStream();
      for (byte[] part : parts) {
        body.writeBytes(part);
      }
      return tlv(0x30, body.toByteArray());
    }

    static byte[] tlv(int tag, byte[] value) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(tag);
      int length = value.length;
      if (length < 0x80) {
        out.write(length);
      } else if (length < 0x100) {
        out.write(0x81);
        out.write(length);
      } else if (length < 0x10000) {
        out.write(0x82);
        out.write(length >> 8);
        out.write(length);
      } else {
        out.write(0x83);
        out.write(length >> 16);
        out.write(length >> 8);
        out.write(length);
      }
      out.writeBytes(value);
      return out.toByteArray();
    }
  }

  /**
   * Accepts every certificate and host name ({@code disableCertificateVerification=true}). It is an
   * {@link X509ExtendedTrustManager} so that the JDK does not add its own host name check.
   */
  private static final class AcceptingTrustManager extends X509ExtendedTrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {}

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {}

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }
}
