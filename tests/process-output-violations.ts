import type { InvariantViolation } from "./invariant-violations.js";

export const UNHANDLED_REJECTION_MARKER = "unhandledRejection: ";

const PROCESS_WARNING_LINE = /^\(node:\d+\) (?:\[[A-Z0-9_]+\] )?([A-Za-z]*Warning): (.*)$/;

const LEADING_ERROR_NAME = /^[A-Za-z]*Error: /;

export function violationOfProcessOutputLine(outputLine: string): InvariantViolation | undefined {
  const warning = PROCESS_WARNING_LINE.exec(outputLine);
  if (warning !== null) {
    return { invariant: "process warning", message: `${warning[1]!}: ${warning[2]!}`, detail: outputLine };
  }
  const markerPosition = outputLine.indexOf(UNHANDLED_REJECTION_MARKER);
  if (markerPosition !== -1) {
    const shownReason = outputLine.slice(markerPosition + UNHANDLED_REJECTION_MARKER.length).trim();
    return {
      invariant: "unhandled rejection",
      message: shownReason.replace(LEADING_ERROR_NAME, ""),
      detail: outputLine,
    };
  }
  return undefined;
}
