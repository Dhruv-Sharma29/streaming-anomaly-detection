#!/usr/bin/env bash
# Download and unpack the toolchain into .tools/ (user space only; `rm -rf .tools` undoes it).
# Every archive is verified against the publisher's checksum before it is unpacked. Idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."
T=.tools; mkdir -p "$T/dl"

JDK_URL="https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.20.1%2B1/OpenJDK17U-jdk_aarch64_linux_hotspot_17.0.20.1_1.tar.gz"
JDK_SHA256="457b57af8f9c93ec39080bb8c764f559dc8c89a6da1a39d718a400b7890d3e41"   # from api.adoptium.net
KAFKA_VERSION=4.3.1; FLINK_VERSION=2.2.1; MAVEN_VERSION=3.9.16; PG_VERSION=16.15-0ubuntu0.24.04.1
APACHE=https://downloads.apache.org

fetch() {  # url file — downloads to .part first so an interrupted download is never mistaken for a complete one
  if [ ! -f "$T/dl/$2" ]; then
    curl -fL --retry 3 -sS -o "$T/dl/$2.part" "$1"
    mv -f "$T/dl/$2.part" "$T/dl/$2"
  fi
}
verify_sha512() {  # file url-of-.sha512
  # Apache projects publish two formats: "<hex>  <file>" (Flink, Maven) and gpg-style
  # "<file>: C7D7B231 8CB51AA0 ..." wrapped over several lines (Kafka). Normalise both.
  local published expected
  published=$(curl -fsSL "$2") || { echo "cannot fetch checksum for $1"; exit 1; }
  expected=$(printf '%s' "$published" | sed 's/^[^:]*: *//' | tr -d ' \t\n' | tr 'A-F' 'a-f' | cut -c1-128)
  if ! [[ "$expected" =~ ^[0-9a-f]{128}$ ]]; then echo "unparseable checksum for $1"; exit 1; fi
  # sha512sum on Linux; macOS ships `shasum -a 512` instead (same check-file format)
  if command -v sha512sum >/dev/null; then SHA512="sha512sum"; else SHA512="shasum -a 512"; fi
  echo "$expected  $T/dl/$1" | $SHA512 -c - >/dev/null || { echo "CHECKSUM MISMATCH: $1"; rm -f "$T/dl/$1"; exit 1; }
  echo "verified sha512: $1"
}

OS="$(uname -s)"
if [ "$OS" = "Darwin" ]; then
  # macOS: Java 17 and PostgreSQL from Homebrew (the bundled downloads below are Linux builds).
  # Kafka, Flink and Maven are JVM tarballs and are downloaded the same way on both systems.
  [ -n "${JAVA_HOME:-}" ] && "$JAVA_HOME/bin/java" -version 2>&1 | grep -q '"17' \
    || { echo 'install Java 17: brew install openjdk@17, then export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"'; exit 1; }
  command -v pg_config >/dev/null || { echo "install PostgreSQL: brew install postgresql@17 (put its bin dir on PATH or set PG_BIN)"; exit 1; }
fi
if [ "$OS" != "Darwin" ] && [ ! -x "$T/jdk/bin/java" ]; then
  fetch "$JDK_URL" jdk.tar.gz
  echo "$JDK_SHA256  $T/dl/jdk.tar.gz" | sha256sum -c - >/dev/null || { echo "CHECKSUM MISMATCH: jdk"; exit 1; }
  mkdir -p "$T/jdk" && tar -xzf "$T/dl/jdk.tar.gz" -C "$T/jdk" --strip-components=1
fi
if [ ! -x "$T/kafka/bin/kafka-server-start.sh" ]; then
  f=kafka_2.13-$KAFKA_VERSION.tgz; fetch "$APACHE/kafka/$KAFKA_VERSION/$f" "$f"
  verify_sha512 "$f" "$APACHE/kafka/$KAFKA_VERSION/$f.sha512"
  mkdir -p "$T/kafka" && tar -xzf "$T/dl/$f" -C "$T/kafka" --strip-components=1
fi
if [ ! -x "$T/flink/bin/flink" ]; then
  f=flink-$FLINK_VERSION-bin-scala_2.12.tgz; fetch "$APACHE/flink/flink-$FLINK_VERSION/$f" "$f"
  verify_sha512 "$f" "$APACHE/flink/flink-$FLINK_VERSION/$f.sha512"
  mkdir -p "$T/flink" && tar -xzf "$T/dl/$f" -C "$T/flink" --strip-components=1
fi
if [ ! -x "$T/maven/bin/mvn" ]; then
  f=apache-maven-$MAVEN_VERSION-bin.tar.gz; fetch "$APACHE/maven/maven-3/$MAVEN_VERSION/binaries/$f" "$f"
  verify_sha512 "$f" "$APACHE/maven/maven-3/$MAVEN_VERSION/binaries/$f.sha512"
  mkdir -p "$T/maven" && tar -xzf "$T/dl/$f" -C "$T/maven" --strip-components=1
fi
if [ "$OS" != "Darwin" ] && [ ! -x "$T/pg/usr/lib/postgresql/16/bin/postgres" ]; then
  mkdir -p "$T/debs" && (cd "$T/debs" && apt-get download "postgresql-16=$PG_VERSION" "postgresql-client-16=$PG_VERSION" "libpq5=$PG_VERSION" 2>/dev/null)
  for deb in "$T"/debs/*.deb; do dpkg-deb -x "$deb" "$T/pg"; done
fi

"${JAVA_HOME:-$T/jdk}/bin/java" -version 2>&1 | head -1
"$T/kafka/bin/kafka-topics.sh" --version 2>/dev/null | head -1 || true
ls "$T/flink/lib" | grep -E '^flink-dist' ; "$T/maven/bin/mvn" -v 2>/dev/null | head -1
if [ "$OS" = "Darwin" ]; then pg_config --version; else "$T/pg/usr/lib/postgresql/16/bin/postgres" --version; fi
