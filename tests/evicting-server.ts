import { startOwnServer } from "./own-server.js";
import type { OwnServer } from "./own-server.js";

export const EVICTING_POLICY_REJECTION =
  'maxmemory-policy is "allkeys-lru": the Valkey or Redis holding the queues must use "noeviction", or it evicts locks and runs';

export async function startEvictingServer({
  isConfigDisabled = false,
}: { isConfigDisabled?: boolean } = {}): Promise<OwnServer> {
  return startOwnServer({
    serverArguments: [
      "--maxmemory-policy",
      "allkeys-lru",
      ...(isConfigDisabled ? ["--rename-command", "CONFIG", ""] : []),
    ],
  });
}
