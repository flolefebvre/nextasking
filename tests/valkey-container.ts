import { GenericContainer, Wait } from "testcontainers";
import type {} from "vitest";
import type { TestProject } from "vitest/node";

declare module "vitest" {
  interface ProvidedContext {
    redisUrl: string;
  }
}

export const DEFAULT_TEST_IMAGE = "valkey/valkey:9";

const REDIS_CONTAINER_PORT = 6379;

export function testImage(env: NodeJS.ProcessEnv = process.env): string {
  return env.NEXTASKING_TEST_IMAGE?.trim() || DEFAULT_TEST_IMAGE;
}

export function providedRedisUrl(env: NodeJS.ProcessEnv = process.env): string | undefined {
  return env.NEXTASKING_TEST_REDIS_URL?.trim() || undefined;
}

export async function provideRedisUrl(project: TestProject): Promise<(() => Promise<void>) | undefined> {
  const reusedRedisUrl = providedRedisUrl();
  if (reusedRedisUrl !== undefined) {
    project.provide("redisUrl", reusedRedisUrl);
    return undefined;
  }

  const container = await new GenericContainer(testImage())
    .withExposedPorts(REDIS_CONTAINER_PORT)
    .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
    .start();
  project.provide(
    "redisUrl",
    `redis://${container.getHost()}:${String(container.getMappedPort(REDIS_CONTAINER_PORT))}`,
  );

  return async () => {
    await container.stop();
  };
}
