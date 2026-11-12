# PROTOTYPE, throwaway: does a connection need a ping?

Not production code. It answers one question and is kept on this branch as the primary source for the answer.

## The question

Does nextasking need an application-level `PING` on its `@redis/client` connections, and if so, should it be the driver's `pingInterval` or a ping the connection module sends and watches itself?

## What it measures

Two hooks, read from the environment, exist only on this branch:

- `PROTOTYPE_DRIVER_PING_MS`: passes `pingInterval` to `createClient` in `src/node-redis-driver.ts`.
- `PROTOTYPE_IDLE_CHECK_MS`: in `src/connection.ts`, sends the existing answer check (`PING`, under the calls-in-flight watchdog) on a ready connection with no call in flight.

Three scenarios, each against `valkey/valkey:9.1.3`:

```sh
docker run -d --rm --name proto-keepalive-vk -p 127.0.0.1:6379:6379 valkey/valkey:9.1.3 --maxmemory-policy noeviction

# A server that closes idle clients (CONFIG SET timeout 3): worker | saturated-worker | dispatcher
pnpm exec tsx scripts/prototype-redis-keepalive/idle-timeout.ts dispatcher 3 10

# A server that answers -BUSY to the ping
PROTOTYPE_DRIVER_PING_MS=1000 pnpm exec tsx scripts/prototype-redis-keepalive/busy-server.ts

# A half-open connection: needs the server on a Docker network named proto-keepalive-net, alias vk
docker network create proto-keepalive-net && docker network connect --alias vk proto-keepalive-net proto-keepalive-vk
sh scripts/prototype-redis-keepalive/half-open.sh "today" 80
```

## What it found (2026-11-12, @redis/client 6.3.1, Node 24.16, Linux 6.6)

Server closes clients idle for 3 s:

| Process                                   | Today                                                                                   | Driver `pingInterval` 1 s | Watched idle check 1 s |
| ----------------------------------------- | --------------------------------------------------------------------------------------- | ------------------------- | ---------------------- |
| Idle worker                               | Both connections send a command every second: never closed                              | not needed                | not needed             |
| Worker with every slot busy               | The blocking connection is closed every 4 s: `connection error` + `connection restored` | not run                   | not run                |
| Process that only dispatches (no worker)  | The command connection is closed every 4 s: `connection error` + `connection restored`  | never closed              | never closed           |

Server answers `-BUSY` to the ping for 3 s:

- Driver `pingInterval`: the driver emits the error reply as an `error` event, the library logs `connection error` and never logs `connection restored`, although the connection never dropped.
- Watched idle check: no line.

Idle connection whose replies are dropped silently (no FIN, no RST):

| Variant                         | Detected after the last answered byte            |
| ------------------------------- | ------------------------------------------------ |
| Today (TCP keepalive, 30 s)     | 42 s (`read ETIMEDOUT`)                          |
| Driver `pingInterval` 10 s      | not within 80 s: the unanswered ping holds off the keepalive probes, and nothing watches it |
| Watched idle check 10 s         | 15 s (10 s to the check, 5 s answer bound)       |

## Verdict

A ping is needed, for the server idle timeout, on connections that go quiet: the command connection of a process that only dispatches, and a worker's blocking connection while every slot is busy. It should be the library's own watched check, not the driver's `pingInterval`.
