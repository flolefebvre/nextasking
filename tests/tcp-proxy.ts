import { randomUUID } from "node:crypto";
import { connect, createServer } from "node:net";
import type { AddressInfo, Server, Socket } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";

export type TcpProxy = {
  url: string;
  cut: () => Promise<void>;
  restore: () => Promise<void>;
  cutFor: (outageDurationInMilliseconds: number) => Promise<void>;
  cutBeforeReplyTo: (isRequestToCut: (request: string) => boolean) => Promise<void>;
  stop: () => Promise<void>;
};

const DEFAULT_REDIS_PORT = 6379;

const LOOPBACK_HOST = "127.0.0.1";

async function listen(server: Server, port: number): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, LOOPBACK_HOST, () => {
      server.removeListener("error", reject);
      resolve();
    });
  });
}

async function closeListener(server: Server): Promise<void> {
  if (!server.listening) {
    return;
  }
  await new Promise<void>((resolve) => server.close(() => resolve()));
}

function echoCommandOf(text: string): string {
  return `*2\r\n$4\r\nECHO\r\n$${Buffer.byteLength(text)}\r\n${text}\r\n`;
}

const CRLF = "\r\n";

const RESP_ARRAY_PREFIX = "*".charCodeAt(0);

function byteLengthOfFirstCompleteCommand(requestBytes: Buffer): number | undefined {
  const headerEnd = requestBytes.indexOf(CRLF);
  if (headerEnd < 0) {
    return undefined;
  }
  if (requestBytes[0] !== RESP_ARRAY_PREFIX) {
    return headerEnd + CRLF.length;
  }
  const argumentCount = Number(requestBytes.toString("latin1", 1, headerEnd));
  let commandEnd = headerEnd + CRLF.length;
  for (let argumentIndex = 0; argumentIndex < argumentCount; argumentIndex += 1) {
    const lengthEnd = requestBytes.indexOf(CRLF, commandEnd);
    if (lengthEnd < 0) {
      return undefined;
    }
    const argumentByteLength = Number(requestBytes.toString("latin1", commandEnd + 1, lengthEnd));
    commandEnd = lengthEnd + CRLF.length + argumentByteLength + CRLF.length;
    if (commandEnd > requestBytes.length) {
      return undefined;
    }
  }
  return commandEnd;
}

type ArmedReplyCut = {
  isRequestToCut: (request: string) => boolean;
  markCut: () => void;
};

export async function startTcpProxy(targetUrl: string): Promise<TcpProxy> {
  const target = new URL(targetUrl);
  const targetPort = target.port === "" ? DEFAULT_REDIS_PORT : Number(target.port);
  const liveSockets = new Set<Socket>();
  let armedReplyCut: ArmedReplyCut | undefined;

  const track = (socket: Socket) => {
    liveSockets.add(socket);
    socket.once("close", () => liveSockets.delete(socket));
  };
  const reset = (socket: Socket) => {
    if (!socket.destroyed) {
      socket.resetAndDestroy();
    }
  };

  const cutOnceRepliesArrive = (targetSocket: Socket, { markCut }: ArmedReplyCut) => {
    const lastReplyMarker = `tcp-proxy-last-reply-${randomUUID()}`;
    let repliesSinceCut = "";
    targetSocket.on("data", (reply: Buffer) => {
      repliesSinceCut += reply.toString();
      if (repliesSinceCut.includes(lastReplyMarker)) {
        void cut().then(markCut);
      }
    });
    targetSocket.resume();
    targetSocket.write(echoCommandOf(lastReplyMarker));
  };

  const server = createServer((clientSocket) => {
    const targetSocket = connect(targetPort, target.hostname);
    track(clientSocket);
    track(targetSocket);
    clientSocket.on("error", () => reset(targetSocket));
    targetSocket.on("error", () => reset(clientSocket));
    clientSocket.on("close", () => reset(targetSocket));
    targetSocket.on("close", () => reset(clientSocket));
    targetSocket.pipe(clientSocket);
    let unforwardedRequestBytes = Buffer.alloc(0);
    let isForwardingRequests = true;
    clientSocket.on("data", (requestChunk: Buffer) => {
      unforwardedRequestBytes = Buffer.concat([unforwardedRequestBytes, requestChunk]);
      let commandByteLength = byteLengthOfFirstCompleteCommand(unforwardedRequestBytes);
      while (isForwardingRequests && commandByteLength !== undefined) {
        const command = unforwardedRequestBytes.subarray(0, commandByteLength);
        unforwardedRequestBytes = unforwardedRequestBytes.subarray(commandByteLength);
        targetSocket.write(command);
        const replyCut = armedReplyCut;
        if (replyCut?.isRequestToCut(command.toString()) === true) {
          armedReplyCut = undefined;
          isForwardingRequests = false;
          targetSocket.unpipe(clientSocket);
          cutOnceRepliesArrive(targetSocket, replyCut);
        }
        commandByteLength = byteLengthOfFirstCompleteCommand(unforwardedRequestBytes);
      }
    });
  });
  await listen(server, 0);
  const proxyPort = (server.address() as AddressInfo).port;
  const proxyUrl = new URL(targetUrl);
  proxyUrl.hostname = LOOPBACK_HOST;
  proxyUrl.port = String(proxyPort);

  const cut = async () => {
    const listenerClosing = closeListener(server);
    for (const socket of liveSockets) {
      reset(socket);
    }
    await listenerClosing;
  };
  const restore = async () => {
    if (!server.listening) {
      await listen(server, proxyPort);
    }
  };

  return {
    url: proxyUrl.toString(),
    cut,
    restore,
    cutFor: async (outageDurationInMilliseconds) => {
      await cut();
      await sleep(outageDurationInMilliseconds);
      await restore();
    },
    cutBeforeReplyTo: (isRequestToCut) =>
      new Promise<void>((markCut) => {
        armedReplyCut = { isRequestToCut, markCut };
      }),
    stop: cut,
  };
}
