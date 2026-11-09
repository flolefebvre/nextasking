import type { ConnectionOutageTracker } from "./connection.js";
import type { DurationInMilliseconds } from "./durations.js";

export type ReadOnlyStretch = {
  noteAnsweredReadOnly: (writeCommandNames: readonly string[], readOnlyReply: Error) => void;
  noteAccepted: (commandNames: readonly string[]) => void;
  forget: () => void;
};

export type ReadOnlyStretchSources = {
  outageTracker: Pick<ConnectionOutageTracker, "reportClientError" | "reportClientReady" | "forgetClient">;
  giveUpAfterInMilliseconds: DurationInMilliseconds;
};

export function createReadOnlyStretch({
  outageTracker,
  giveUpAfterInMilliseconds,
}: ReadOnlyStretchSources): ReadOnlyStretch {
  const stretchSubject = {};
  const learnedWriteCommandNames = new Set<string>();
  let giveUpTimer: NodeJS.Timeout | undefined;

  const end = () => {
    clearTimeout(giveUpTimer);
    giveUpTimer = undefined;
    learnedWriteCommandNames.clear();
  };

  return {
    noteAnsweredReadOnly: (writeCommandNames, readOnlyReply) => {
      for (const commandName of writeCommandNames) {
        learnedWriteCommandNames.add(commandName);
      }
      if (giveUpTimer === undefined) {
        giveUpTimer = setTimeout(() => {
          outageTracker.reportClientError(stretchSubject, readOnlyReply);
        }, giveUpAfterInMilliseconds);
        giveUpTimer.unref();
      }
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
