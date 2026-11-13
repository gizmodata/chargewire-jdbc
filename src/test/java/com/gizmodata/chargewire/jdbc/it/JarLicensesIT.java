package com.gizmodata.chargewire.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.chargewire.jdbc.ChargewireDriver;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.Test;

/**
 * The licence texts in the jar that is released (these tests run against it): one LICENSE, one
 * NOTICE, a listing of the third-party components, and the text of every licence the listing names.
 * The listing must name exactly the components the jar contains, in the versions it contains, so a
 * dependency that is added or updated without its licence fails the build. No server is involved.
 */
class JarLicensesIT {
  private static final Pattern LISTED =
      Pattern.compile("^([\\w.\\-]+:[\\w.\\-]+:[\\w.\\-]+) \\| ([\\w.\\-]+) \\| (META-INF/\\S+)$");

  private static final Pattern PATH = Pattern.compile("META-INF/[\\w./\\-]*[\\w\\-]");

  private static File jar() throws Exception {
    File jar =
        new File(
            ChargewireDriver.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    assertTrue(
        jar.isFile() && jar.getName().endsWith(".jar"),
        "the integration tests run against the standalone jar, not " + jar);
    return jar;
  }

  private static String text(ZipFile zip, String name) throws IOException {
    ZipEntry entry = zip.getEntry(name);
    assertNotNull(entry, name + " is not in the jar");
    try (InputStream in = zip.getInputStream(entry)) {
      String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(text.strip().length() > 100, name + " is empty");
      return text;
    }
  }

  @Test
  void theListingNamesExactlyWhatTheJarContains() throws Exception {
    try (ZipFile zip = new ZipFile(jar())) {
      // What the jar contains: each bundled Maven artifact left its pom.properties; zstd-jni,
      // which isn't built with Maven, says its version in a class.
      TreeSet<String> contained = new TreeSet<>();
      for (ZipEntry entry : Collections.list(zip.entries())) {
        if (entry.getName().matches("META-INF/maven/[^/]+/[^/]+/pom\\.properties")) {
          Properties properties = new Properties();
          try (InputStream in = zip.getInputStream(entry)) {
            properties.load(in);
          }
          contained.add(
              properties.getProperty("groupId")
                  + ":"
                  + properties.getProperty("artifactId")
                  + ":"
                  + properties.getProperty("version"));
        }
      }
      assertTrue(
          contained.removeIf(artifact -> artifact.startsWith("com.gizmodata:chargewire-jdbc:")),
          "the driver's own pom.properties");
      assertNotNull(zip.getEntry("com/github/luben/zstd/util/ZstdVersion.class"));
      contained.add("com.github.luben:zstd-jni:" + com.github.luben.zstd.util.ZstdVersion.VERSION);

      String listing = text(zip, "META-INF/THIRD-PARTY-NOTICES.txt");
      TreeSet<String> listed = new TreeSet<>();
      for (String line : listing.split("\n")) {
        Matcher matcher = LISTED.matcher(line.trim());
        if (matcher.matches()) {
          listed.add(matcher.group(1));
        }
      }
      assertEquals(
          contained,
          listed,
          "META-INF/THIRD-PARTY-NOTICES.txt must list every component in the jar, in its version"
              + " (first: what the jar contains)");

      // Every licence file the listing names, anywhere in its text, is in the jar.
      Matcher path = PATH.matcher(listing);
      int named = 0;
      while (path.find()) {
        text(zip, path.group());
        named++;
      }
      assertTrue(named >= listed.size(), "the listing names " + named + " files");
    }
  }

  @Test
  void theJarHasOneLicenceAndOneNotice() throws Exception {
    try (ZipFile zip = new ZipFile(jar())) {
      String licence = text(zip, "META-INF/LICENSE");
      assertTrue(licence.contains("Apache License"), "META-INF/LICENSE is the Apache License");
      assertTrue(licence.contains("Version 2.0, January 2004"));
      assertEquals(Files.readString(Path.of("LICENSE")), licence, "the repository's LICENSE");
      String notice = text(zip, "META-INF/NOTICE");
      assertEquals(Files.readString(Path.of("src/main/legal/NOTICE")), notice);
      assertTrue(notice.startsWith("Chargewire JDBC driver\nCopyright"), notice.substring(0, 60));
      for (String name :
          List.of(
              "META-INF/LICENSE.txt",
              "META-INF/LICENSE.md",
              "META-INF/NOTICE.txt",
              "META-INF/NOTICE.md",
              "META-INF/DEPENDENCIES")) {
        assertEquals(null, zip.getEntry(name), name + " of some component got into the jar");
      }
      for (String name :
          List.of(
              "META-INF/licenses/slf4j/LICENSE.txt",
              "META-INF/licenses/zstd-jni/LICENSE",
              "META-INF/licenses/zstd/LICENSE",
              "META-INF/FastDoubleParser-LICENSE",
              "META-INF/FastDoubleParser-ThirdParty-LICENSE",
              "META-INF/Schubfach-LICENSE")) {
        text(zip, name);
      }
    }
  }
}
