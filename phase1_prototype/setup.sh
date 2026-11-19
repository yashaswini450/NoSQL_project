#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
STATE_ROOT="${NASA_STATE_DIR:-$HOME/.local/share/nasa-phase1}"
mkdir -p "$STATE_ROOT"/{downloads,data,logs,staging}
if ! test -x /usr/lib/postgresql/16/bin/initdb; then
  sudo apt-get update -qq
  sudo apt-get install -y postgresql-16 python3-venv curl ca-certificates
fi
fetch() {
  local url="$1" file="$2" digest="$3"
  if ! test -f "$file"; then curl --fail --location --retry 3 "$url" -o "$file.part"; mv "$file.part" "$file"; fi
  echo "$digest  $file" | sha256sum --check --status
}
if ! test -x "$STATE_ROOT/mongodb/bin/mongod"; then
  fetch 'https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2404-8.0.38.tgz' "$STATE_ROOT/downloads/mongodb.tgz" '2885d4382bd47b96bfaf3edc7dec0ef44463e3e2a79479f584d20deb6182cda9'
  mkdir -p "$STATE_ROOT/mongodb"
  tar -xzf "$STATE_ROOT/downloads/mongodb.tgz" --strip-components=1 -C "$STATE_ROOT/mongodb"
fi
if ! test -x "$STATE_ROOT/java/bin/java"; then
  fetch 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.33%2B10/OpenJDK11U-jre_x64_linux_hotspot_11.0.33_10.tar.gz' "$STATE_ROOT/downloads/java.tgz" '038086c264806875f3610317acc3455ce8daaf73561349c514efd646bb24a6e9'
  mkdir -p "$STATE_ROOT/java"
  tar -xzf "$STATE_ROOT/downloads/java.tgz" --strip-components=1 -C "$STATE_ROOT/java"
fi
# Official Hadoop client API + shaded runtime contain the genuine LocalJobRunner.
# Streaming runs that engine directly; a full HDFS/YARN server distribution is unnecessary.
mkdir -p "$STATE_ROOT/hadoop"
for artifact in \
  org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar \
  org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar \
  org/apache/hadoop/hadoop-streaming/3.3.6/hadoop-streaming-3.3.6.jar \
  org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar \
  org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar \
  commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar \
  commons-cli/commons-cli/1.2/commons-cli-1.2.jar \
  commons-io/commons-io/2.8.0/commons-io-2.8.0.jar; do
  jar="$STATE_ROOT/hadoop/${artifact##*/}"
  url="https://repo.maven.apache.org/maven2/$artifact"
  if ! test -f "$jar"; then
    curl -fsSL --retry 3 "$url" -o "$jar.part"
    mv "$jar.part" "$jar"
  fi
  digest=$(curl -fsSL --retry 3 "$url.sha1")
  echo "$digest  $jar" | sha1sum --check --status
done
test -x "$STATE_ROOT/venv/bin/python" || python3 -m venv "$STATE_ROOT/venv"
"$STATE_ROOT/venv/bin/pip" install -q -r requirements.txt
echo "Setup complete: $STATE_ROOT"
bash services.sh start
