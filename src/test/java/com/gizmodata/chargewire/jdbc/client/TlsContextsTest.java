package com.gizmodata.chargewire.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * TLS set-up: PEM keys and the trust sources (ported from {@code ClientAuthenticationUtilsTest}).
 */
class TlsContextsTest {
  private static Path cert(String name) {
    return Path.of("src", "test", "resources", "certs", name);
  }

  @ParameterizedTest
  @CsvSource({
    "client-key.pem, RSA",
    "client-key-pkcs1.pem, RSA",
    "ec-key-sec1.pem, EC",
    "ec-key-pkcs8.pem, EC"
  })
  void readsPemPrivateKeys(String file, String algorithm) throws Exception {
    PrivateKey key = TlsContexts.readPrivateKey(cert(file));
    assertEquals(algorithm, key.getAlgorithm());
  }

  @Test
  void pkcs1AndPkcs8OfTheSameKeyAgree() throws Exception {
    assertEquals(
        TlsContexts.readPrivateKey(cert("client-key.pem")),
        TlsContexts.readPrivateKey(cert("client-key-pkcs1.pem")));
    ECPrivateKey pkcs8 = (ECPrivateKey) TlsContexts.readPrivateKey(cert("ec-key-pkcs8.pem"));
    ECPrivateKey sec1 = (ECPrivateKey) TlsContexts.readPrivateKey(cert("ec-key-sec1.pem"));
    assertEquals(pkcs8.getS(), sec1.getS(), "the same private scalar");
    assertEquals(pkcs8.getParams().getCurve(), sec1.getParams().getCurve(), "on the same curve");
  }

  @Test
  void refusesEncryptedAndMissingKeys(@TempDir Path directory) throws Exception {
    Path encrypted = directory.resolve("encrypted.pem");
    Files.writeString(
        encrypted,
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----\n");
    GeneralSecurityException error =
        assertThrows(GeneralSecurityException.class, () -> TlsContexts.readPrivateKey(encrypted));
    assertTrue(error.getMessage().contains("openssl pkcs8"), error.getMessage());
    assertThrows(
        GeneralSecurityException.class,
        () -> TlsContexts.readPrivateKey(cert("ca.pem")),
        "a certificate is not a key");
  }

  private static ConnectionConfig config(
      boolean system,
      Path roots,
      Path trustStore,
      String password,
      Path clientCertificate,
      Path clientKey,
      boolean insecure) {
    return new ConnectionConfig.Builder()
        .address("localhost", 1)
        .tls(true, insecure, system, roots, trustStore, password, clientCertificate, clientKey)
        .build();
  }

  @Test
  void buildsContextsForEachTrustSource() throws Exception {
    assertNotNull(TlsContexts.create(config(true, null, null, null, null, null, false)));
    assertNotNull(TlsContexts.create(config(false, cert("ca.pem"), null, null, null, null, false)));
    assertNotNull(
        TlsContexts.create(
            config(false, null, cert("truststore.p12"), "changeit", null, null, false)));
    assertNotNull(
        TlsContexts.create(config(false, null, null, null, null, null, true)),
        "skipping verification needs no trust source");
    assertNotNull(
        TlsContexts.create(
            config(
                false,
                cert("ca.pem"),
                null,
                null,
                cert("client.pem"),
                cert("client-key.pem"),
                false)));
  }

  @Test
  void refusesAnImpossibleTrustSetUp() {
    GeneralSecurityException none =
        assertThrows(
            GeneralSecurityException.class,
            () -> TlsContexts.create(config(false, null, null, null, null, null, false)));
    assertTrue(none.getMessage().contains("no trusted certificates"));
    assertThrows(
        GeneralSecurityException.class,
        () ->
            TlsContexts.create(
                config(false, cert("client-key.pem"), null, null, null, null, false)),
        "a key is not a certificate");
    GeneralSecurityException half =
        assertThrows(
            GeneralSecurityException.class,
            () ->
                TlsContexts.create(
                    config(true, null, null, null, cert("client.pem"), null, false)));
    assertTrue(half.getMessage().contains("both clientCertificate and clientKey"));
  }
}
