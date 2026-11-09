import { connect } from "node:net";
import type { Socket } from "node:net";

import { expect, vi } from "vitest";

import { integrationTest } from "./integration-test.js";
import { startTcpProxy } from "./tcp-proxy.js";

async function connectedSocketOf(url: string): Promise<Socket> {
  const { hostname, port } = new URL(url);
  const socket = connect(Number(port), hostname);
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  return socket;
}

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
