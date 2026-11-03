import { connect, createServer } from "node:net";
import type { AddressInfo, Socket } from "node:net";

export type TcpProxy = {
  url: string;
  cut: () => void;
  restore: () => void;
  stop: () => Promise<void>;
};

const DEFAULT_REDIS_PORT = 6379;

export async function startTcpProxy(targetUrl: string): Promise<TcpProxy> {
  const target = new URL(targetUrl);
  const targetPort = target.port === "" ? DEFAULT_REDIS_PORT : Number(target.port);
  const liveSockets = new Set<Socket>();
  let isCut = false;

  const track = (socket: Socket) => {
    liveSockets.add(socket);
    socket.once("close", () => liveSockets.delete(socket));
  };
  const reset = (socket: Socket) => {
    if (!socket.destroyed) {
      socket.resetAndDestroy();
    }
  };

  const server = createServer((clientSocket) => {
    if (isCut) {
      reset(clientSocket);
      return;
    }
    const targetSocket = connect(targetPort, target.hostname);
    track(clientSocket);
    track(targetSocket);
    clientSocket.on("error", () => reset(targetSocket));
    targetSocket.on("error", () => reset(clientSocket));
    clientSocket.on("close", () => reset(targetSocket));
    targetSocket.on("close", () => reset(clientSocket));
    clientSocket.pipe(targetSocket);
    targetSocket.pipe(clientSocket);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const proxyUrl = new URL(targetUrl);
  proxyUrl.hostname = "127.0.0.1";
  proxyUrl.port = String((server.address() as AddressInfo).port);

  return {
    url: proxyUrl.toString(),
    cut: () => {
      isCut = true;
      for (const socket of liveSockets) {
        reset(socket);
      }
    },
    restore: () => {
      isCut = false;
    },
    stop: async () => {
      isCut = true;
      for (const socket of liveSockets) {
        reset(socket);
      }
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
