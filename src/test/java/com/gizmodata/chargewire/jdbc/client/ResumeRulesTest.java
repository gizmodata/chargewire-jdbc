package com.gizmodata.chargewire.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** The waits around a resuming server: {@code Retry-After}, and the requests' own timeout. */
class ResumeRulesTest {
  @ParameterizedTest
  @CsvSource({"0, 1", "1, 1", "' 7 ', 7", "60, 60", "61, 60", "86400, 60"})
  void retryAfterIsSecondsFromOneToSixty(String header, long seconds) {
    assertEquals(
        Duration.ofSeconds(seconds), ChargewireClient.resumeRetryAfter(/* retryAfter= */ header));
  }

  @ParameterizedTest
  @CsvSource({"-5", "soon", "'Wed, 21 Oct 2026 07:28:00 GMT'", "''"})
  void anUnusableRetryAfterWaitsThreeSeconds(String header) {
    assertEquals(Duration.ofSeconds(3), ChargewireClient.resumeRetryAfter(header));
  }

  @Test
  void noRetryAfterWaitsThreeSeconds() {
    assertEquals(Duration.ofSeconds(3), ChargewireClient.resumeRetryAfter((String) null));
  }

  @Test
  void everyRequestWaitsAtLeastThirtySeconds() {
    assertEquals(
        Duration.ofSeconds(30), ChargewireClient.heldTimeout(/* requestTimeout= */ secs(10)));
    assertEquals(Duration.ofSeconds(30), ChargewireClient.heldTimeout(secs(30)));
    assertEquals(Duration.ofSeconds(90), ChargewireClient.heldTimeout(secs(90)));
    assertNull(ChargewireClient.heldTimeout(null));
  }

  private static Duration secs(long seconds) {
    return Duration.ofSeconds(seconds);
  }
}
