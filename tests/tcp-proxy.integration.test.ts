import { connect, createServer } from "node:net";
import type { Socket } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";

import { expect, vi } from "vitest";

import { integrationTest } from "./integration-test.js";
import { startTcpProxy } from "./tcp-proxy.js";
import type { TcpProxy } from "./tcp-proxy.js";
import { withRedis } from "./with-redis.js";
import type { TestClient } from "./with-redis.js";

async function connectedSocketOf(url: string): Promise<Socket> {
  const { hostname, port } = new URL(url);
  const socket = connect(Number(port), hostname);
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  return socket;
}

function setCommandOf(key: string): string {
  return `*3\r\n$3\r\nSET\r\n$${String(Buffer.byteLength(key))}\r\n${key}\r\n$1\r\n1\r\n`;
}

const WINDOW_FOR_A_SILENCED_REQUEST_TO_ARRIVE_IN_MILLISECONDS = 500;

type SocketsThroughAProxy = {
  proxy: TcpProxy;
  redis: TestClient;
  connectedSocket: () => Promise<Socket>;
};

async function withSocketsThroughAProxy(
  redisUrl: string,
  use: (socketsThroughAProxy: SocketsThroughAProxy) => Promise<void>,
): Promise<void> {
  const proxy = await startTcpProxy(redisUrl);
  const sockets: Socket[] = [];
  const connectedSocket = async () => {
    const socket = await connectedSocketOf(proxy.url);
    sockets.push(socket);
    return socket;
  };
  try {
    await withRedis(redisUrl, (redis) => use({ proxy, redis, connectedSocket }));
  } finally {
    for (const socket of sockets) {
      socket.destroy();
    }
    await proxy.stop();
  }
}

integrationTest(
  "a request sent on a silenced connection never reaches the server, while one held on a connection accepted during the silence reaches it once the proxy answers again",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withSocketsThroughAProxy(redisUrl, async ({ proxy, redis, connectedSocket }) => {
      const silencedSocket = await connectedSocket();
      await vi.waitFor(() => expect(proxy.acceptedConnectionCount()).toBe(1), { timeout: 5_000 });

      proxy.stopAnswering();
      silencedSocket.write(setCommandOf(`${keyPrefix}silenced`));
      const heldSocket = await connectedSocket();
      heldSocket.write(setCommandOf(`${keyPrefix}held`));
      proxy.answerAgain();
      await vi.waitFor(async () => expect(await redis.get(`${keyPrefix}held`)).toBe("1"), { timeout: 5_000 });
      await sleep(WINDOW_FOR_A_SILENCED_REQUEST_TO_ARRIVE_IN_MILLISECONDS);

      expect(await redis.get(`${keyPrefix}silenced`)).toBeNull();
    });
  },
);

integrationTest(
  "a held request and the ones after it on its connection reach the server only once released, while other connections go through",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withSocketsThroughAProxy(redisUrl, async ({ proxy, redis, connectedSocket }) => {
      const heldSocket = await connectedSocket();
      const otherSocket = await connectedSocket();
      const heldRequest = proxy.holdRequest((request) => request.includes(`${keyPrefix}held`));

      heldSocket.write(setCommandOf(`${keyPrefix}before`));
      heldSocket.write(setCommandOf(`${keyPrefix}held`));
      heldSocket.write(setCommandOf(`${keyPrefix}after`));
      const { release } = await heldRequest;
      otherSocket.write(setCommandOf(`${keyPrefix}other`));
      await vi.waitFor(async () => expect(await redis.get(`${keyPrefix}other`)).toBe("1"), { timeout: 5_000 });
      await sleep(WINDOW_FOR_A_SILENCED_REQUEST_TO_ARRIVE_IN_MILLISECONDS);
      const valuesWhileHeld = await redis.mGet([`${keyPrefix}before`, `${keyPrefix}held`, `${keyPrefix}after`]);
      release();
      await vi.waitFor(async () => expect(await redis.get(`${keyPrefix}after`)).toBe("1"), { timeout: 5_000 });

      expect(valuesWhileHeld).toEqual(["1", null, null]);
      expect(await redis.get(`${keyPrefix}held`)).toBe("1");
    });
  },
);

integrationTest(
  "silencedConnectionCount counts the connections live when the proxy stopped answering until their client closes them",
  { timeout: 25_000 },
  async ({ redisUrl }) => {
    const proxy = await startTcpProxy(redisUrl);
    const sockets: Socket[] = [];
    try {
      const commandSocket = await connectedSocketOf(proxy.url);
      const blockingSocket = await connectedSocketOf(proxy.url);
      sockets.push(commandSocket, blockingSocket);
      await vi.waitFor(() => expect(proxy.acceptedConnectionCount()).toBe(2), { timeout: 5_000 });

      const countBeforeTheSilence = proxy.silencedConnectionCount();
      proxy.stopAnswering();
      const reconnectingSocket = await connectedSocketOf(proxy.url);
      sockets.push(reconnectingSocket);
      await vi.waitFor(() => expect(proxy.acceptedConnectionCount()).toBe(3), { timeout: 5_000 });
      const countOnceSilenced = proxy.silencedConnectionCount();
      commandSocket.write("*1\r\n$4\r\nPING\r\n");
      commandSocket.destroy();
      await vi.waitFor(() => expect(proxy.silencedConnectionCount()).toBe(1), { timeout: 5_000 });
      proxy.answerAgain();
      const countOnceAnsweringAgain = proxy.silencedConnectionCount();
      blockingSocket.destroy();
      await vi.waitFor(() => expect(proxy.silencedConnectionCount()).toBe(0), { timeout: 5_000 });

      expect(countBeforeTheSilence).toBe(0);
      expect(countOnceSilenced).toBe(2);
      expect(countOnceAnsweringAgain).toBe(1);
    } finally {
      for (const socket of sockets) {
        socket.destroy();
      }
      await proxy.stop();
    }
  },
);

async function errorCodeOnceClosedOf(socket: Socket): Promise<string | undefined> {
  let errorCode: string | undefined;
  socket.on("error", (error: NodeJS.ErrnoException) => {
    errorCode = error.code;
  });
  await new Promise<void>((resolve) => socket.once("close", () => resolve()));
  return errorCode;
}

async function replyToPingThrough(socket: Socket): Promise<string> {
  const reply = new Promise<string>((resolve) => socket.once("data", (chunk: Buffer) => resolve(chunk.toString())));
  socket.write("*1\r\n$4\r\nPING\r\n");
  return reply;
}

async function listenErrorCodeOnPortOf(url: string): Promise<string | undefined> {
  const listener = createServer();
  const errorCode = await new Promise<string | undefined>((resolve) => {
    listener.once("error", (error: NodeJS.ErrnoException) => resolve(error.code));
    listener.listen(Number(new URL(url).port), new URL(url).hostname, () => resolve(undefined));
  });
  await new Promise<void>((resolve) => listener.close(() => resolve()));
  return errorCode;
}

integrationTest(
  "a cut proxy keeps its port: a listener asked for it during the cut is refused, a connection made during the cut is reset, and the restore lets the next one through",
  { timeout: 15_000 },
  async ({ redisUrl }) => {
    const proxy = await startTcpProxy(redisUrl);
    const sockets: Socket[] = [];
    try {
      await proxy.cut();
      const listenErrorCodeDuringTheCut = await listenErrorCodeOnPortOf(proxy.url);
      const socketDuringTheCut = connect(Number(new URL(proxy.url).port), new URL(proxy.url).hostname);
      sockets.push(socketDuringTheCut);
      const errorCodeOfASocketDuringTheCut = await errorCodeOnceClosedOf(socketDuringTheCut);
      await proxy.restore();
      const socketAfterTheRestore = await connectedSocketOf(proxy.url);
      sockets.push(socketAfterTheRestore);
      const replyAfterTheRestore = await replyToPingThrough(socketAfterTheRestore);

      expect(listenErrorCodeDuringTheCut).toBe("EADDRINUSE");
      expect(errorCodeOfASocketDuringTheCut).toBe("ECONNRESET");
      expect(replyAfterTheRestore).toBe("+PONG\r\n");
      expect(proxy.acceptedConnectionCount()).toBe(1);
    } finally {
      for (const socket of sockets) {
        socket.destroy();
      }
      await proxy.stop();
    }
  },
);

integrationTest(
  "a retarget resets the connections live through the proxy and sends every later connection to the new target",
  { timeout: 15_000 },
  async ({ redisUrl }) => {
    const retargetedProxy = await startTcpProxy(redisUrl);
    const newTargetProxy = await startTcpProxy(redisUrl);
    const sockets: Socket[] = [];
    try {
      const liveSocket = await connectedSocketOf(retargetedProxy.url);
      sockets.push(liveSocket);
      const liveSocketErrorCode = errorCodeOnceClosedOf(liveSocket);
      await vi.waitFor(() => expect(retargetedProxy.acceptedConnectionCount()).toBe(1), { timeout: 5_000 });

      retargetedProxy.retarget(newTargetProxy.url);
      const laterSocket = await connectedSocketOf(retargetedProxy.url);
      sockets.push(laterSocket);
      const laterReply = await replyToPingThrough(laterSocket);

      expect(await liveSocketErrorCode).toBe("ECONNRESET");
      expect(laterReply).toBe("+PONG\r\n");
      expect(newTargetProxy.acceptedConnectionCount()).toBe(1);
      expect(retargetedProxy.targetUrl()).toBe(newTargetProxy.url);
    } finally {
      for (const socket of sockets) {
        socket.destroy();
      }
      await retargetedProxy.stop();
      await newTargetProxy.stop();
    }
  },
);
