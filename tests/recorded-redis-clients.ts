import type * as RedisClientModule from "@redis/client";

type RedisClientModuleType = typeof RedisClientModule;

type RecordedRedisClient = ReturnType<RedisClientModuleType["createClient"]>;

const clientRecords: { url: string | undefined; client: RecordedRedisClient }[] = [];

export function redisClientModuleRecordingClients(importedModule: unknown): RedisClientModuleType {
  const actualModule = importedModule as RedisClientModuleType;
  const createClient = ((...createArguments: Parameters<RedisClientModuleType["createClient"]>) => {
    const client = actualModule.createClient(...createArguments);
    clientRecords.push({ url: createArguments[0]?.url, client });
    return client;
  }) as RedisClientModuleType["createClient"];
  return { ...actualModule, createClient };
}

export function socketOptionsOfClientsFor(url: string): unknown[] {
  return clientRecords.filter((clientRecord) => clientRecord.url === url).map(({ client }) => client.options?.socket);
}

export function listenerCountsByEventOfClientsFor(url: string): Record<string, number>[] {
  return clientRecords
    .filter((clientRecord) => clientRecord.url === url)
    .map(({ client }) =>
      Object.fromEntries(client.eventNames().map((eventName) => [String(eventName), client.listenerCount(eventName)])),
    );
}
