#!/usr/bin/env bash
# Checks a Maven Central bundle (target/central-publishing/central-bundle.zip, from
# `mvn -P release deploy -Dcentral.skipUpload=true`) against what Central requires before it is
# uploaded: for the POM, the jar, the sources jar and the Javadoc jar, each file present and
# non-empty, a GPG signature (.asc) that verifies (by the expected key, if given), and .md5/.sha1
# (plus .sha256/.sha512) checksums that match; the POM's required metadata (name, description,
# url, licenses, developers, scm) and version; no SNAPSHOT dependency; a jar that registers a
# java.sql.Driver and carries this version; and nothing else in the bundle.
#
# Usage: dev/check-central-bundle.sh <bundle.zip> <groupId> <artifactId> <version> [<key fingerprint>]
# The signing key's public half must be in gpg's keyring (GNUPGHOME). With CENTRAL_BUNDLE_DEPENDENCIES
# set (space-separated groupId:artifactId:version; empty for none), the POM's compile and runtime
# dependencies must be exactly those.
set -euo pipefail

[ $# -ge 4 ] || { echo "usage: $0 <bundle.zip> <groupId> <artifactId> <version> [<key fingerprint>]" >&2; exit 2; }
bundle=$1 group=$2 artifact=$3 version=$4 fingerprint=${5:-}
[ -s "$bundle" ] || { echo "error: no bundle at $bundle" >&2; exit 1; }
[ -n "$version" ] || { echo "error: empty version" >&2; exit 1; }
case "$version" in *-SNAPSHOT) echo "error: $version is a SNAPSHOT; Central takes releases only" >&2; exit 1 ;; esac

work=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/central-bundle.XXXXXX")
trap 'rm -rf "$work"' EXIT
unzip -q "$bundle" -d "$work"

dir="${group//.//}/$artifact/$version"
base="$dir/$artifact-$version"
failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

expected=()
for file in "$base.pom" "$base.jar" "$base-sources.jar" "$base-javadoc.jar"; do
  for suffix in "" .asc .md5 .sha1 .sha256 .sha512; do
    expected+=("$file$suffix")
  done
done

# Exactly the expected files: nothing missing, nothing extra.
actual=$(cd "$work" && find . -type f | sed 's:^\./::' | sort)
wanted=$(printf '%s\n' "${expected[@]}" | sort)
if [ "$actual" != "$wanted" ]; then
  fail "the bundle's files differ from the expected set"
  diff <(echo "$wanted") <(echo "$actual") >&2 || true
fi

checksum() { # algorithm file
  case "$1" in
    md5) openssl dgst -md5 -r "$2" ;;
    sha1) openssl dgst -sha1 -r "$2" ;;
    sha256) openssl dgst -sha256 -r "$2" ;;
    sha512) openssl dgst -sha512 -r "$2" ;;
  esac | cut -d' ' -f1
}

for file in "$base.pom" "$base.jar" "$base-sources.jar" "$base-javadoc.jar"; do
  path="$work/$file"
  [ -s "$path" ] || { fail "$file is missing or empty"; continue; }
  for algorithm in md5 sha1 sha256 sha512; do
    [ -s "$path.$algorithm" ] || { fail "$file.$algorithm is missing"; continue; }
    want=$(tr -d ' \n\r' < "$path.$algorithm")
    have=$(checksum "$algorithm" "$path")
    [ "$want" = "$have" ] || fail "$file.$algorithm does not match the file"
  done
  if [ ! -s "$path.asc" ]; then
    fail "$file.asc is missing"
    continue
  fi
  status=$(gpg --batch --status-fd 1 --verify "$path.asc" "$path" 2>/dev/null || true)
  signer=$(awk '$2 == "VALIDSIG" { print $3; exit }' <<<"$status")
  if [ -z "$signer" ]; then
    fail "$file.asc does not verify"
  elif [ -n "$fingerprint" ] && [ "$signer" != "$fingerprint" ]; then
    fail "$file.asc is signed by $signer, not $fingerprint"
  fi
done

# The POM: Central's required metadata, the version, and no SNAPSHOT dependency.
if [ -s "$work/$base.pom" ]; then
  python3 -I - "$work/$base.pom" "$group" "$artifact" "$version" "${CENTRAL_BUNDLE_DEPENDENCIES-*}" <<'PY' || failures=$((failures + 1))
import sys
import xml.etree.ElementTree as ET

path, group, artifact, version, expected_dependencies = sys.argv[1:6]
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
root = ET.parse(path).getroot()
problems = []

def text(xpath):
    node = root.find(xpath, ns)
    return (node.text or "").strip() if node is not None else ""

for field, want in (("m:groupId", group), ("m:artifactId", artifact), ("m:version", version)):
    if text(field) != want:
        problems.append(f"{field[2:]} is {text(field)!r}, expected {want!r}")
for field in ("m:name", "m:description", "m:url", "m:licenses/m:license/m:name",
              "m:licenses/m:license/m:url", "m:developers/m:developer/m:name",
              "m:scm/m:url", "m:scm/m:connection"):
    if not text(field):
        problems.append(f"{field.replace('m:', '')} is missing")
# Consumers resolve ${...} in a dependency from the POM's own properties: do the same.
properties = {node.tag.split("}", 1)[1]: (node.text or "").strip()
              for node in root.findall("m:properties/*", ns)}
properties["project.version"] = text("m:version")
properties["project.groupId"] = text("m:groupId")

def resolve(value):
    for _ in range(10):
        start = value.find("${")
        end = value.find("}", start)
        if start < 0 or end < 0 or value[start + 2:end] not in properties:
            break
        value = value[:start] + properties[value[start + 2:end]] + value[end + 1:]
    return value

runtime = set()
for dependency in root.findall("m:dependencies/m:dependency", ns):
    coordinates = ":".join(resolve(dependency.findtext(f"m:{f}", "", ns)) for f in ("groupId", "artifactId", "version"))
    scope = dependency.findtext("m:scope", "compile", ns)
    if "SNAPSHOT" in coordinates or "${" in coordinates:
        problems.append(f"dependency {coordinates} is not a fixed release")
    if scope not in ("test", "provided"):
        print(f"dependency: {coordinates} ({scope})")
        runtime.add(coordinates)
if expected_dependencies != "*" and runtime != set(expected_dependencies.split()):
    problems.append(f"compile/runtime dependencies are {sorted(runtime)}, expected {sorted(expected_dependencies.split())}")
for problem in problems:
    print(f"FAIL: POM: {problem}", file=sys.stderr)
sys.exit(1 if problems else 0)
PY
fi

# The jar registers a JDBC driver and carries this version; the other jars have content.
if [ -s "$work/$base.jar" ]; then
  if ! unzip -p "$work/$base.jar" META-INF/services/java.sql.Driver > "$work/drivers.txt" 2>/dev/null \
      || [ ! -s "$work/drivers.txt" ]; then
    fail "$artifact-$version.jar registers no java.sql.Driver"
  fi
  manifest=$(unzip -p "$work/$base.jar" META-INF/MANIFEST.MF | tr -d '\r')
  [[ $manifest == *"Implementation-Version: $version"* ]] \
    || fail "$artifact-$version.jar's manifest lacks Implementation-Version: $version"
fi
if [ -s "$work/$base-sources.jar" ]; then
  [[ $(unzip -Z1 "$work/$base-sources.jar") == *.java* ]] || fail "the sources jar has no .java files"
fi
if [ -s "$work/$base-javadoc.jar" ]; then
  [[ $'\n'$(unzip -Z1 "$work/$base-javadoc.jar")$'\n' == *$'\n'index.html$'\n'* ]] \
    || fail "the Javadoc jar has no index.html"
fi

if [ "$failures" -ne 0 ]; then
  echo "$bundle: $failures problem(s)" >&2
  exit 1
fi
echo "$bundle: $group:$artifact:$version OK (pom, jar, sources, javadoc; signatures and checksums verified${fingerprint:+; signed by $fingerprint})"
