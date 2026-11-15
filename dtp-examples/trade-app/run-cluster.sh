#!/usr/bin/env bash
# Copyright 2026 os890
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Starts an H2 server (localhost only) and N instances of the trade app on it, ports 8081..808N
# (PORT_BASE+1..PORT_BASE+N with PORT_BASE set, e.g. PORT_BASE=8090 if 8081 is taken).
# Build first: mvn install (from the repository root). Stop with ./stop-cluster.sh.
# Usage: [PORT_BASE=8090] [JAVA_OPTS="-D..."] ./run-cluster.sh [nodes]
#   JAVA_OPTS: extra system properties of all nodes, e.g. placeholders of the job definitions (applied on a new DB)
set -euo pipefail
cd "$(dirname "$0")"

NODES=${1:-3}
H2_VERSION=${H2_VERSION:-2.4.240}
H2_JAR="${HOME}/.m2/repository/com/h2database/h2/${H2_VERSION}/h2-${H2_VERSION}.jar"
DATA=cluster-data
APP=target/quarkus-app/quarkus-run.jar

[ -f "$APP" ] || { echo "missing $APP, run 'mvn install' first" >&2; exit 1; }
mkdir -p "$DATA"

java -cp "$H2_JAR" org.h2.tools.Server -tcp -tcpPort 9092 -ifNotExists -baseDir "$DATA" > "$DATA/h2.log" 2>&1 &
echo $! > "$DATA/h2.pid"
sleep 2
echo "H2 server on port 9092 (data in $DATA)"

for i in $(seq 1 "$NODES"); do
  port=$((${PORT_BASE:-8080} + i))
  # shellcheck disable=SC2086
  java ${JAVA_OPTS:-} -Dquarkus.profile=cluster -Dquarkus.http.port="$port" -Ddtp.node-name="node-$i" -jar "$APP" \
    > "$DATA/node-$i.log" 2>&1 &
  echo $! > "$DATA/node-$i.pid"
  # start the nodes one after the other, so the schema is created once
  until curl -sf "http://localhost:$port/q/health/ready" > /dev/null; do sleep 1; done
  echo "node-$i ready on http://localhost:$port"
done
