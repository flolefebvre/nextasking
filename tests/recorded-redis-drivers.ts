import type * as NodeRedisDriverModule from "../src/node-redis-driver.js";
import type { RedisDriver } from "../src/node-redis-driver.js";

type NodeRedisDriverModuleType = typeof NodeRedisDriverModule;

const driverRecords: { url: string; driver: RedisDriver }[] = [];

export function nodeRedisDriverModuleRecordingDrivers(importedModule: unknown): NodeRedisDriverModuleType {
  const actualModule = importedModule as NodeRedisDriverModuleType;
  const createNodeRedisDriver: NodeRedisDriverModuleType["createNodeRedisDriver"] = (url, events) => {
    const driver = actualModule.createNodeRedisDriver(url, events);
    driverRecords.push({ url, driver });
    return driver;
  };
  return { ...actualModule, createNodeRedisDriver };
}

export function driversCreatedFor(url: string): RedisDriver[] {
  return driverRecords.filter((driverRecord) => driverRecord.url === url).map(({ driver }) => driver);
}
