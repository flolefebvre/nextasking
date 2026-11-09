export type HandlerGate = {
  enter: () => Promise<void>;
  release: (count: number) => void;
  inFlightCount: () => number;
  mostInFlightCount: () => number;
};

export function createHandlerGate(): HandlerGate {
  const waitingReleases: (() => void)[] = [];
  let inFlightCount = 0;
  let mostInFlightCount = 0;
  return {
    enter: async () => {
      inFlightCount += 1;
      mostInFlightCount = Math.max(mostInFlightCount, inFlightCount);
      await new Promise<void>((resolve) => waitingReleases.push(resolve));
      inFlightCount -= 1;
    },
    release: (count) => {
      for (const release of waitingReleases.splice(0, count)) {
        release();
      }
    },
    inFlightCount: () => inFlightCount,
    mostInFlightCount: () => mostInFlightCount,
  };
}
