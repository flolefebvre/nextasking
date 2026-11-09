import { createHash } from "node:crypto";

import {
  ClientClosedError,
  ConnectionTimeoutError,
  createClient,
  DisconnectsClientError,
  ErrorReply,
  SocketClosedUnexpectedlyError,
  TimeoutError,
} from "@redis/client";

import { commandResultOf } from "./redis-client.js";
import type { CommandResult, RedisCommand, RedisScript } from "./redis-client.js";

export type RedisDriverEvents = {
  onConnect: () => void;
  onReady: () => void;
  onError: (error: Error) => void;
};

export type RedisDriver = {
  readonly isReady: boolean;
  connect: () => void;
  forceReconnect: () => void;
  destroy: () => void;
  close: () => Promise<void>;
  send: (command: RedisCommand, abortSignal?: AbortSignal) => Promise<unknown>;
  sendBatch: (commands: readonly RedisCommand[], abortSignal?: AbortSignal) => Promise<CommandResult[]>;
};

export type CreateRedisDriver = (url: string, events: RedisDriverEvents) => RedisDriver;

const NETWORK_ERROR_CODES = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "ECONNABORTED",
  "ETIMEDOUT",
  "EPIPE",
  "ENOTFOUND",
  "EAI_AGAIN",
  "EHOSTUNREACH",
  "ENETUNREACH",
]);

const UNANSWERED_ERROR_CLASSES = [
  SocketClosedUnexpectedlyError,
  DisconnectsClientError,
  TimeoutError,
  ConnectionTimeoutError,
  ClientClosedError,
];

const NO_SCRIPT_REPLY_PREFIX = "NOSCRIPT";

const READ_ONLY_REPLY_PREFIX = "READONLY";

const OUT_OF_MEMORY_REPLY_PATTERN = /(?:^|[\s:-])OOM command not allowed\b/;

export function isUnansweredCommandError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  const { code } = error as { code?: unknown };
  return (
    UNANSWERED_ERROR_CLASSES.some((errorClass) => error instanceof errorClass) ||
    (typeof code === "string" && NETWORK_ERROR_CODES.has(code))
  );
}

export function isErrorReply(error: unknown): error is Error {
  return error instanceof ErrorReply;
}

export function isOutOfMemoryReply(error: unknown): boolean {
  return error instanceof ErrorReply && OUT_OF_MEMORY_REPLY_PATTERN.test(error.message);
}

export function isReadOnlyReply(error: unknown): boolean {
  return error instanceof ErrorReply && error.message.startsWith(READ_ONLY_REPLY_PREFIX);
}

export function unansweredWithinDeadlineError(): Error {
  return new TimeoutError();
}

function isNoScriptReply(error: unknown): boolean {
  return error instanceof ErrorReply && error.message.startsWith(NO_SCRIPT_REPLY_PREFIX);
}

const shaByLua = new Map<string, string>();

function shaOf({ lua }: RedisScript): string {
  let sha = shaByLua.get(lua);
  if (sha === undefined) {
    sha = createHash("sha1").update(lua).digest("hex");
    shaByLua.set(lua, sha);
  }
  return sha;
}

export const createNodeRedisDriver: CreateRedisDriver = (url, { onConnect, onReady, onError }) => {
  const client = createClient({
    url,
    RESP: 2,
    maintNotifications: "disabled",
    commandOptions: { timeout: undefined },
  });
  client.on("connect", onConnect);
  client.on("ready", onReady);
  client.on("error", onError);

  const sentCommand = (commandArguments: readonly (string | number)[], abortSignal: AbortSignal | undefined) =>
    client.sendCommand(
      commandArguments.map(String),
      abortSignal === undefined ? undefined : { abortSignal },
    ) as Promise<unknown>;

  const send = async (command: RedisCommand, abortSignal?: AbortSignal): Promise<unknown> => {
    if (command.kind === "builtIn") {
      return sentCommand([command.name, ...command.commandArguments], abortSignal);
    }
    const { script, keys, scriptArguments } = command;
    const keysAndArguments = [script.keyCount, ...keys, ...scriptArguments];
    try {
      return await sentCommand(["EVALSHA", shaOf(script), ...keysAndArguments], abortSignal);
    } catch (error) {
      if (!isNoScriptReply(error)) {
        throw error;
      }
      return sentCommand(["EVAL", script.lua, ...keysAndArguments], abortSignal);
    }
  };

  let isDestroyed = false;
  const connect = () => {
    if (!isDestroyed) {
      client.connect().catch(() => undefined);
    }
  };

  return {
    get isReady() {
      return client.isReady;
    },
    connect,
    forceReconnect: () => {
      client.destroy();
      setImmediate(connect);
    },
    destroy: () => {
      isDestroyed = true;
      client.destroy();
    },
    close: () => client.close(),
    send,
    sendBatch: (commands, abortSignal) =>
      Promise.all(commands.map((command) => commandResultOf(send(command, abortSignal)))),
  };
};
