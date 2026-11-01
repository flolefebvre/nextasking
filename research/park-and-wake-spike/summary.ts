import { readdirSync, readFileSync } from "node:fs";

const filter = process.argv.slice(2);
for (const file of readdirSync("results").sort()) {
  if (filter.length && !filter.some((f) => file.startsWith(f))) continue;
  const r = JSON.parse(readFileSync(`results/${file}`, "utf8"));
  if (!r.perKey) continue;
  console.log(r.id, "recheck", r.recheck, "elapsed", r.elapsedMs, "stuck", r.stuck, "stalled", r.stalledEvents, "failed", r.failedEvents, "kills", r.kills, "counts", JSON.stringify(r.finalCounts));
  console.log("   stats", JSON.stringify(r.stats));
  const entries = Object.entries<any>(r.perKey);
  if (entries.length > 5) {
    const vs = entries.map(([, v]) => v);
    console.log("   keys", vs.length, "maxConcurrent", Math.max(...vs.map((v) => v.maxConcurrentFromIntervals)), "violations", vs.reduce((a, v) => a + v.violations.length, 0), "fallbacks>1s", vs.reduce((a, v) => a + v.releasesFallingBackOver1s, 0), "maxGap", Math.max(...vs.map((v) => v.releaseToNextStartMs.max ?? 0)), "gaps>1s", JSON.stringify(vs.map((v) => v.releaseToNextStartMs.max).filter((g) => g > 1000).sort((a, b) => a - b)));
    continue;
  }
  for (const [k, v] of entries) {
    console.log("  ", k, "exec", v.executions, "outOfOrder", v.runsStartedBeforeAnOlderRun, "maxOvertaken", v.maxPositionsOvertaken, "killed", v.killedExecutions, "maxI", v.maxConcurrentFromIntervals, "maxC", v.maxConcurrentFromCounter, "viol", JSON.stringify(v.violations), "gap", JSON.stringify(v.releaseToNextStartMs), "fb>1s", v.releasesFallingBackOver1s, "startDelay", JSON.stringify(v.startDelayFromDispatchMs), "first", v.firstStartMs, "lastEnd", v.lastEndMs);
  }
}
