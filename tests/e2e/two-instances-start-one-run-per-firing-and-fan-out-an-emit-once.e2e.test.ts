import { expect, vi } from "vitest";

import { deriveFiringRunId } from "../../src/ids.js";
import { e2eTest } from "./e2e-test.js";
import { fixtureRecordsOf } from "../fixture-records.js";
import { startNext } from "./next-process.js";
import type { NextProcess } from "./next-process.js";
import { submitServerActionForm } from "./server-action-form.js";

type FiringRecord = { runId: string; scheduledAt: string };

const FIRINGS_TO_WAIT_FOR = 3;

const INVOICE_PAID_LISTENER_NAMES = ["record-invoice-payment", "send-receipt"];

function firingRecordsByScheduledAt(firingRecords: readonly FiringRecord[]): Map<string, FiringRecord[]> {
  const recordsByScheduledAt = new Map<string, FiringRecord[]>();
  for (const firingRecord of firingRecords) {
    recordsByScheduledAt.set(firingRecord.scheduledAt, [
      ...(recordsByScheduledAt.get(firingRecord.scheduledAt) ?? []),
      firingRecord,
    ]);
  }
  return recordsByScheduledAt;
}

async function waitForWorkerStarted(nextProcess: NextProcess): Promise<void> {
  await expect.poll(() => nextProcess.output(), { timeout: 15_000 }).toContain('"message":"worker started"');
}

e2eTest(
  "two instances on one prefix start one run per firing of a schedule, and fan out one emit once",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnvWithScheduleOnAndGracefulStop }) => {
    const nextProcesses: NextProcess[] = [];

    try {
      for (let instance = 0; instance < 2; instance += 1) {
        const nextProcess = await startNext(fixtureAppDir, {
          command: "start",
          env: fixtureAppEnvWithScheduleOnAndGracefulStop,
        });
        nextProcesses.push(nextProcess);
        await waitForWorkerStarted(nextProcess);
      }
      const bothWorkersStartedAt = Date.now();
      const response = await submitServerActionForm(`${nextProcesses[0]?.url ?? ""}/invoices`, { invoiceId: "inv-1" });
      const firingRecordsSinceBothStarted = async () =>
        ((await fixtureRecordsOf(redisUrl, keyPrefix, "refresh-exchange-rates")) as FiringRecord[]).filter(
          (firingRecord) => Date.parse(firingRecord.scheduledAt) > bothWorkersStartedAt,
        );
      await vi.waitFor(
        async () => {
          const recordsByScheduledAt = firingRecordsByScheduledAt(await firingRecordsSinceBothStarted());
          expect(recordsByScheduledAt.size).toBeGreaterThanOrEqual(FIRINGS_TO_WAIT_FOR);
        },
        { timeout: 30_000, interval: 500 },
      );
      const firingRecords = await fixtureRecordsOf(redisUrl, keyPrefix, "refresh-exchange-rates");
      const listenerRecords = await Promise.all(
        INVOICE_PAID_LISTENER_NAMES.map((listenerName) => fixtureRecordsOf(redisUrl, keyPrefix, listenerName)),
      );

      expect(response.status).toBe(200);
      for (const [scheduledAt, recordsOfOneFiring] of firingRecordsByScheduledAt(firingRecords as FiringRecord[])) {
        expect(recordsOfOneFiring).toEqual([
          { runId: deriveFiringRunId("refresh-exchange-rates", Date.parse(scheduledAt)), scheduledAt },
        ]);
      }
      const oneListenerRun = [{ runId: expect.any(String), payload: { invoiceId: "inv-1" } }];
      expect(listenerRecords).toEqual(INVOICE_PAID_LISTENER_NAMES.map(() => oneListenerRun));
      const exits = await Promise.all(nextProcesses.map((nextProcess) => nextProcess.stop()));
      expect(exits).toEqual(nextProcesses.map(() => ({ code: 0, signal: null, wasForced: false })));
    } finally {
      await Promise.all(nextProcesses.map((nextProcess) => nextProcess.stop()));
    }
  },
);
