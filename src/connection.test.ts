import { ReplyError } from "ioredis";
import { describe, expect, test } from "vitest";

import { createConnectionOutageTracker, isUnansweredError, reconnectDecisionOn } from "./connection.js";
import { createRecordingLogger } from "../tests/recording-logger.js";

function networkError(code: string): Error {
  return Object.assign(new Error(`connect ${code} 127.0.0.1:6379`), { code });
}

describe("the connection outage tracker", () => {
  test("logs one error and one restored line for an outage that every client goes through, flapping included", () => {
    const { logger, lines } = createRecordingLogger();
    const outageTracker = createConnectionOutageTracker(logger);
    const instanceClient = {};
    const workerClient = {};
    const connectionReset = networkError("ECONNRESET");

    outageTracker.reportClientError(instanceClient, connectionReset);
    outageTracker.reportClientError(workerClient, networkError("ECONNREFUSED"));
    outageTracker.reportClientReady(instanceClient);
    outageTracker.reportClientError(instanceClient, networkError("ECONNREFUSED"));
    outageTracker.reportClientReady(workerClient);
    outageTracker.reportClientReady(instanceClient);

    expect(lines).toEqual([
      { level: "error", fields: { err: connectionReset }, message: "connection error" },
      { level: "info", fields: {}, message: "connection restored" },
    ]);
  });

  test("logs a second pair for a second outage", () => {
    const { logger, lines } = createRecordingLogger();
    const outageTracker = createConnectionOutageTracker(logger);
    const client = {};

    outageTracker.reportClientError(client, networkError("ECONNREFUSED"));
    outageTracker.reportClientReady(client);
    outageTracker.reportClientError(client, networkError("ECONNREFUSED"));
    outageTracker.reportClientReady(client);

    expect(lines.map((line) => line.message)).toEqual([
      "connection error",
      "connection restored",
      "connection error",
      "connection restored",
    ]);
  });

  test("logs nothing for a client that becomes ready without an error", () => {
    const { logger, lines } = createRecordingLogger();
    const outageTracker = createConnectionOutageTracker(logger);

    outageTracker.reportClientReady({});

    expect(lines).toEqual([]);
  });

  test("calls each connection-restored listener once per outage, when it logs connection restored, until it unsubscribes", () => {
    const { logger, lines } = createRecordingLogger();
    const outageTracker = createConnectionOutageTracker(logger);
    const instanceClient = {};
    const workerClient = {};
    const messagesLoggedWhenRestored: string[][] = [];
    const stopListening = outageTracker.onConnectionRestored(() => {
      messagesLoggedWhenRestored.push(lines.map((line) => line.message));
    });

    outageTracker.reportClientReady(instanceClient);
    outageTracker.reportClientError(instanceClient, networkError("ECONNRESET"));
    outageTracker.reportClientError(workerClient, networkError("ECONNRESET"));
    outageTracker.reportClientReady(instanceClient);
    outageTracker.reportClientReady(workerClient);
    stopListening();
    outageTracker.reportClientError(workerClient, networkError("ECONNRESET"));
    outageTracker.reportClientReady(workerClient);

    expect(messagesLoggedWhenRestored).toEqual([["connection error", "connection restored"]]);
  });

  test("ends an outage silently when the last client still down is closed, and logs the next one", () => {
    const { logger, lines } = createRecordingLogger();
    const outageTracker = createConnectionOutageTracker(logger);
    const closedClient = {};
    const otherClient = {};

    outageTracker.reportClientError(closedClient, networkError("ECONNREFUSED"));
    outageTracker.forgetClient(closedClient);
    outageTracker.reportClientError(otherClient, networkError("ECONNREFUSED"));

    expect(lines.map((line) => line.message)).toEqual(["connection error", "connection error"]);
  });
});

describe("reconnectDecisionOn", () => {
  test("reconnects and resends the command on a READONLY reply, as during a failover", () => {
    expect(reconnectDecisionOn(new ReplyError("READONLY You can't write against a read only replica."))).toBe(2);
  });

  test("leaves the connection alone on any other error", () => {
    expect(reconnectDecisionOn(new ReplyError("ERR unknown command 'CONFIG'"))).toBe(false);
  });
});

describe("isUnansweredError", () => {
  test.each([
    ["a command timeout", new Error("Command timed out")],
    ["a closed connection", new Error("Connection is closed.")],
    ["a pipeline command aborted by a closed connection", new Error("Command aborted due to connection close")],
    ["a socket timeout", new Error("Socket timeout. Expecting data, but didn't receive any in 5000ms.")],
    ["a refused connection", networkError("ECONNREFUSED")],
    ["a reset connection", networkError("ECONNRESET")],
    ["an unknown host", networkError("ENOTFOUND")],
    [
      "exhausted retries",
      Object.assign(new Error("Reached the max retries per request limit"), {
        name: "MaxRetriesPerRequestError",
      }),
    ],
  ])("is true for %s", (_case, error) => {
    expect(isUnansweredError(error)).toBe(true);
  });

  test.each([
    ["a reply error", new ReplyError("ERR wrong number of arguments")],
    ["an error of the call itself", new Error("Job is not in failed state")],
    ["a value that isn't an error", "Command timed out"],
  ])("is false for %s", (_case, error) => {
    expect(isUnansweredError(error)).toBe(false);
  });
});
