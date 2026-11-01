import { startWorker } from "@flefebvre/nextasking";
import { nextasking } from "@/jobs/nextasking";

await startWorker(nextasking);
