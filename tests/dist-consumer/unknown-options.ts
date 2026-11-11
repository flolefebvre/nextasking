import type { StartWorkerOptions } from "@flefebvre/nextasking";

import { nextasking } from "./registered-instance.js";
import type { Expect, IsEqual } from "./type-equality.js";

declare const isDraining: boolean;

interface StartWorkerOptionsWithATenant extends StartWorkerOptions {
  tenant: string;
}

const unknownWorkerOptions = { concurency: 1 };

const workerOptionsWithOneKnown = { concurency: 1, shutdownTimeout: "10s" as const };

declare const workerOptionsWiderThanTheirType: StartWorkerOptionsWithATenant;

export async function startWorkerWithUnknownOptions(): Promise<void> {
  // @ts-expect-error concurency is not an option of startWorker
  await nextasking.startWorker({ concurency: 1 });
  // @ts-expect-error concurency is not an option of startWorker
  await nextasking.startWorker(unknownWorkerOptions);
  // @ts-expect-error concurency is not an option of startWorker
  await nextasking.startWorker(workerOptionsWithOneKnown);
  // @ts-expect-error concurency is not an option of startWorker
  await nextasking.startWorker({ shutdownTimeout: "10s", ...unknownWorkerOptions });
  // @ts-expect-error tenant is not an option of startWorker
  await nextasking.startWorker(workerOptionsWiderThanTheirType);
  // @ts-expect-error a string is not the options
  await nextasking.startWorker("10s");
  // @ts-expect-error a list is not the options
  await nextasking.startWorker(["10s"]);
  // @ts-expect-error null is not the options
  await nextasking.startWorker(null);
  // @ts-expect-error concurrency is a number
  await nextasking.startWorker({ concurrency: "5" });
}

declare const typedWorkerOptions: StartWorkerOptions;

declare const partialWorkerOptions: Partial<StartWorkerOptions>;

declare const readonlyWorkerOptions: Readonly<StartWorkerOptions>;

const knownWorkerOptions = { shutdownTimeout: "10s" } as const;

export async function startWorkerWithKnownOptions(): Promise<void> {
  await nextasking.startWorker(typedWorkerOptions);
  await nextasking.startWorker({ concurrency: 1, ...knownWorkerOptions });
  await nextasking.startWorker({ ...typedWorkerOptions, concurrency: 2 });
  await nextasking.startWorker(isDraining ? { shutdownTimeout: "1m" } : {});
  await nextasking.startWorker(partialWorkerOptions);
  await nextasking.startWorker(readonlyWorkerOptions);
  await nextasking.startWorker({ concurrency: undefined, onWorkerNotStarted: (error) => void error });
  await nextasking.startWorker(undefined);
}

export const startWorkerAsAFunctionOfItsOptions: (options?: StartWorkerOptions) => Promise<void> =
  nextasking.startWorker;

export type StartWorkerAtItsDefaultTakesItsOptions = Expect<
  IsEqual<Parameters<typeof nextasking.startWorker<StartWorkerOptions>>, [options?: StartWorkerOptions]>
>;

export type StartWorkerGivenTheMembersOfAStringTakesItsOptions = Expect<
  IsEqual<
    Parameters<typeof nextasking.startWorker<{ [Member in keyof string]: unknown }>>,
    [options?: StartWorkerOptions]
  >
>;

export type StartWorkerGivenAListTakesItsOptions = Expect<
  IsEqual<Parameters<typeof nextasking.startWorker<string[]>>, [options?: StartWorkerOptions]>
>;
