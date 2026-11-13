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

# Stops the nodes gracefully (drain), then the H2 server. Add --wipe to delete the database.
set -uo pipefail
cd "$(dirname "$0")"
DATA=cluster-data

for pid_file in "$DATA"/node-*.pid; do
  [ -f "$pid_file" ] && kill "$(cat "$pid_file")" 2>/dev/null
done
sleep 5
[ -f "$DATA/h2.pid" ] && kill "$(cat "$DATA/h2.pid")" 2>/dev/null
rm -f "$DATA"/*.pid
if [ "${1:-}" = "--wipe" ]; then
  rm -rf "$DATA"
fi
echo "cluster stopped"
