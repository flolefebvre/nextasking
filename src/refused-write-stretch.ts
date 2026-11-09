import type { ConnectionOutageTracker } from "./connection.js";
import type { DurationInMilliseconds } from "./durations.js";

export type RefusedWriteStretch = {
  noteAnsweredReadOnly: (writeCommandNames: readonly string[], readOnlyReply: Error) => void;
  noteAnsweredOutOfMemory: (writeCommandNames: readonly string[], outOfMemoryReply: Error) => void;
  noteAccepted: (commandNames: readonly string[]) => void;
  forget: () => void;
};

export type RefusedWriteStretchSources = {
  outageTracker: Pick<ConnectionOutageTracker, "reportClientError" | "reportClientReady" | "forgetClient">;
  giveUpAfterInMilliseconds: DurationInMilliseconds;
};

export function createRefusedWriteStretch({
  outageTracker,
  giveUpAfterInMilliseconds,
}: RefusedWriteStretchSources): RefusedWriteStretch {
  const stretchSubject = {};
  const learnedWriteCommandNames = new Set<string>();
  let readOnlyGiveUpTimer: NodeJS.Timeout | undefined;

  const learn = (writeCommandNames: readonly string[]) => {
    for (const commandName of writeCommandNames) {
      learnedWriteCommandNames.add(commandName);
    }
  };

  const end = () => {
    clearTimeout(readOnlyGiveUpTimer);
    readOnlyGiveUpTimer = undefined;
    learnedWriteCommandNames.clear();
  };

  return {
    noteAnsweredReadOnly: (writeCommandNames, readOnlyReply) => {
      learn(writeCommandNames);
      if (readOnlyGiveUpTimer === undefined) {
        readOnlyGiveUpTimer = setTimeout(() => {
          outageTracker.reportClientError(stretchSubject, readOnlyReply);
        }, giveUpAfterInMilliseconds);
        readOnlyGiveUpTimer.unref();
      }
    },
    noteAnsweredOutOfMemory: (writeCommandNames, outOfMemoryReply) => {
      learn(writeCommandNames);
      outageTracker.reportClientError(stretchSubject, outOfMemoryReply);
    },
    noteAccepted: (commandNames) => {
      if (commandNames.some((commandName) => learnedWriteCommandNames.has(commandName))) {
        end();
        outageTracker.reportClientReady(stretchSubject);
      }
    },
    forget: () => {
      end();
      outageTracker.forgetClient(stretchSubject);
    },
  };
}
