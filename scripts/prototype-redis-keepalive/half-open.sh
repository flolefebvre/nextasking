#!/bin/sh
# PROTOTYPE, throwaway. Runs the idle connection in a container next to Valkey, then drops every
# packet Valkey sends back, without a FIN or a RST: a half-open connection.
# Usage: half-open.sh <label> <observe seconds> [ENV=value ...]
set -eu
label=$1
observe=$2
shift 2
here=$(cd "$(dirname "$0")/../.." && pwd)
network=proto-keepalive-net
server=proto-keepalive-vk
client=proto-keepalive-client-$$

env_flags=""
for pair in "$@"; do env_flags="$env_flags -e $pair"; done

docker rm -f "$client" >/dev/null 2>&1 || true
# shellcheck disable=SC2086
docker run -d --name "$client" --network "$network" -v "$here":/app -w /app $env_flags node:24-slim \
  node --import tsx scripts/prototype-redis-keepalive/half-open.ts "$observe" >/dev/null

echo "=== $label"
until docker logs "$client" 2>&1 | grep -q OPEN; do sleep 0.2; done
sleep 2
subnet=$(docker inspect "$client" --format "{{(index .NetworkSettings.Networks \"$network\").IPAddress}}")/32
docker run --rm --net "container:$server" --cap-add NET_ADMIN alpine:3 ip route add blackhole "$subnet"
echo "$(date -u +%H:%M:%S.%3N)  BLACKHOLE: Valkey's replies to $subnet are now dropped silently"
docker wait "$client" >/dev/null
docker logs "$client" 2>&1
docker run --rm --net "container:$server" --cap-add NET_ADMIN alpine:3 ip route del blackhole "$subnet"
docker rm -f "$client" >/dev/null
