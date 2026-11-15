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

# Starts a local PostgreSQL for dtp in Podman (127.0.0.1:${PG_PORT:-5433}, database/user dtp, password dtp), building
# the image from the Containerfile next to this script on first use. Stop with: ./run.sh stop
set -euo pipefail
cd "$(dirname "$0")"
IMAGE=localhost/dtp-postgresql:17
NAME=dtp-postgresql
PORT=${PG_PORT:-5433}

if [ "${1:-start}" = "stop" ]; then
  podman rm -f "$NAME" > /dev/null 2>&1 || true
  echo "stopped $NAME"
  exit 0
fi
podman image exists "$IMAGE" || podman build -t "$IMAGE" -f Containerfile .
podman rm -f "$NAME" > /dev/null 2>&1 || true
podman run -d --name "$NAME" --pull=never -p "127.0.0.1:$PORT:5432" "$IMAGE" > /dev/null
until podman exec "$NAME" pg_isready -q -h 127.0.0.1 -U dtp -d postgres 2>/dev/null; do sleep 1; done
podman exec -e PGPASSWORD=dtp "$NAME" psql -q -h 127.0.0.1 -U dtp -d postgres -c "CREATE DATABASE dtp" > /dev/null
echo "PostgreSQL ready: jdbc:postgresql://127.0.0.1:$PORT/dtp (user dtp, password dtp)"
