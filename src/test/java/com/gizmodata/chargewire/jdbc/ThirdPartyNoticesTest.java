package com.gizmodata.chargewire.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.Test;

/**
 * The licence files the jar ships, against the jars the build resolves: every component {@code
 * META-INF/THIRD-PARTY-NOTICES.txt} lists is on the class path in exactly that version, its own
 * NOTICE is in the jar's NOTICE word for word, and its licence text is the one shipped for it. So a
 * dependency update that changes a version, a notice or a licence fails here until the files
 * follow. ({@code JarLicensesIT} checks the other direction on the built jar: that everything in it
 * is listed.)
 */
class ThirdPartyNoticesTest {
  /** A listed component. */
  record Component(String group, String artifact, String version, String licence, String text) {
    String jarName() {
      return artifact + "-" + version + ".jar";
    }

    @Override
    public String toString() {
      return group + ":" + artifact + ":" + version;
    }
  }

  private static final Pattern LINE =
      Pattern.compile(
          "^([\\w.\\-]+):([\\w.\\-]+):([\\w.\\-]+) \\| ([\\w.\\-]+) \\| (META-INF/\\S+)$");

  private static final String TERMS_END = "END OF TERMS AND CONDITIONS";

  /** Reads the listing: {@code groupId:artifactId:version | licence | path of its text}. */
  static List<Component> listed(String thirdPartyNotices) {
    List<Component> components = new ArrayList<>();
    for (String line : thirdPartyNotices.split("\n")) {
      Matcher matcher = LINE.matcher(line.trim());
      if (matcher.matches()) {
        components.add(
            new Component(
                matcher.group(1),
                matcher.group(2),
                matcher.group(3),
                matcher.group(4),
                matcher.group(5)));
      }
    }
    return components;
  }

  private static String resource(String path) throws IOException {
    try (InputStream in = ThirdPartyNoticesTest.class.getClassLoader().getResourceAsStream(path)) {
      assertNotNull(in, path + " is not among the driver's resources");
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /** Line endings and trailing blanks aside, the text as it is. */
  private static String lines(String text) {
    return text.replace("\r\n", "\n").replaceAll("[ \t]+\n", "\n").strip();
  }

  /** The licence's terms, without its appendix (whose example line a project fills in). */
  private static String terms(String apacheLicence) {
    int end = apacheLicence.indexOf(TERMS_END);
    assertTrue(end > 0, "not the Apache License 2.0 text");
    return apacheLicence.substring(0, end).replaceAll("\\s+", " ").strip();
  }

  private static File jarOnClassPath(Component component) {
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      File file = new File(entry);
      if (file.getName().equals(component.jarName())) {
        return file;
      }
    }
    return null;
  }

  private static String entry(ZipFile jar, String... names) throws IOException {
    for (String name : names) {
      ZipEntry entry = jar.getEntry(name);
      if (entry != null) {
        try (InputStream in = jar.getInputStream(entry)) {
          return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
      }
    }
    return null;
  }

  @Test
  void everyListedComponentIsResolvedInThatVersionWithItsNoticeAndLicence() throws IOException {
    List<Component> components = listed(resource("META-INF/THIRD-PARTY-NOTICES.txt"));
    assertTrue(components.size() >= 17, "the listing has " + components.size() + " components");
    String notice = lines(Files.readString(Path.of("src/main/legal/NOTICE")));
    String apache = Files.readString(Path.of("LICENSE"));
    for (Component component : components) {
      File jar = jarOnClassPath(component);
      assertNotNull(
          jar,
          component
              + " is listed in THIRD-PARTY-NOTICES.txt, but the build resolves no "
              + component.jarName()
              + ": update the line, and the component's notice and licence text with it");
      try (ZipFile zip = new ZipFile(jar)) {
        String itsNotice = entry(zip, "META-INF/NOTICE", "META-INF/NOTICE.txt");
        if (itsNotice != null) {
          assertTrue(
              notice.contains("\n" + component + "\n"),
              "src/main/legal/NOTICE has no section for " + component);
          assertTrue(
              notice.contains(lines(itsNotice)),
              "src/main/legal/NOTICE does not carry the NOTICE of " + component + " word for word");
        }
        String itsLicence = entry(zip, "META-INF/LICENSE", "META-INF/LICENSE.txt");
        if (component.licence().equals("Apache-2.0")) {
          assertEquals("META-INF/LICENSE", component.text(), component.toString());
          if (itsLicence != null && itsLicence.contains(TERMS_END)) {
            assertEquals(terms(apache), terms(itsLicence), "the licence of " + component);
          } else if (itsLicence != null) {
            // A pointer to the licence instead of its text (jackson-datatype-jsr310).
            assertTrue(
                itsLicence.contains("Apache") && itsLicence.contains("2.0"),
                component + " says its licence is: " + itsLicence);
          }
        } else {
          byte[] shipped =
              Files.readAllBytes(Path.of("src/main/resources").resolve(component.text()));
          assertTrue(shipped.length > 0, component.text());
          if (itsLicence != null) {
            assertArrayEquals(
                itsLicence.getBytes(StandardCharsets.UTF_8),
                shipped,
                component.text() + " is not the licence text inside " + component.jarName());
          }
        }
      }
    }
  }

  /** The texts that come from no jar say what they are, and name the version they were taken at. */
  @Test
  void theLicencesNoJarCarriesAreTheOnesListed() throws IOException {
    String listing = resource("META-INF/THIRD-PARTY-NOTICES.txt");
    String zstdJni = resource("META-INF/licenses/zstd-jni/LICENSE");
    assertTrue(zstdJni.contains("Luben Karavelov"), zstdJni);
    assertTrue(zstdJni.contains("BSD-2-Clause"), zstdJni);
    assertTrue(zstdJni.contains("Redistributions in binary form must reproduce"), zstdJni);
    String zstd = resource("META-INF/licenses/zstd/LICENSE");
    assertTrue(zstd.contains("For Zstandard software"), zstd);
    assertTrue(zstd.contains("Neither the name Facebook"), zstd);
    String slf4j = resource("META-INF/licenses/slf4j/LICENSE.txt");
    assertTrue(slf4j.contains("QOS.ch"), slf4j);
    assertTrue(slf4j.contains("Permission is hereby granted, free  of charge"), slf4j);
    Component zstdJniListed =
        listed(listing).stream().filter(c -> c.artifact().equals("zstd-jni")).findFirst().get();
    assertEquals("BSD-2-Clause", zstdJniListed.licence());
    assertTrue(
        listing.contains("at tag v" + zstdJniListed.version() + " "),
        "the listing says which zstd-jni tag its licence texts were taken at, and that is the"
            + " version bundled: fetch LICENSE and src/main/native/LICENSE again at tag v"
            + zstdJniListed.version());
    assertTrue(
        listing.contains("zstd-jni " + zstdJniListed.version() + " carries"),
        "and so does the Zstandard paragraph");
  }

  @Test
  void theJarsNoticeStartsWithTheProjectsOwn() throws IOException {
    String own = lines(Files.readString(Path.of("NOTICE")));
    String shipped = lines(Files.readString(Path.of("src/main/legal/NOTICE")));
    assertTrue(shipped.startsWith(own + "\n"), "src/main/legal/NOTICE starts with NOTICE");
    assertFalse(own.isEmpty());
  }
}
