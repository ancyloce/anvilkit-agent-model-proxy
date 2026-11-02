// The error vocabulary of the call surface: the caller principal, a refusal
// answered as an error envelope with its HTTP status (contracts.md §4), and
// the mapping of Control's denials and refusals onto those codes.

import type { ControlRefused } from "../adapters/control.js";
import type { ErrorCode } from "../contracts.js";

export interface Principal {
	id: string;
	kind: "workflow" | "sidecar" | "control";
}

/** A refusal answered as an error envelope (no stream was opened). */
export class CallError extends Error {
	constructor(
		readonly code: ErrorCode,
		message: string,
		readonly retryable = false,
	) {
		super(message);
	}
}

/** The HTTP status of an error code on this surface (contracts.md §4). */
export function statusOf(code: ErrorCode): number {
	switch (code) {
		case "INVALID_ARGUMENT":
			return 400;
		case "UNAUTHENTICATED":
			return 401;
		case "FORBIDDEN":
		case "STALE_EXECUTION":
		case "PROFILE_UNQUALIFIED":
			return 403;
		case "NOT_FOUND":
			return 404;
		case "IDEMPOTENCY_CONFLICT":
			return 409;
		case "BUDGET_EXHAUSTED":
			return 429;
		default:
			return 503;
	}
}

export function denialErrorCode(code: string): ErrorCode {
	switch (code) {
		case "STALE_EXECUTION":
		case "PROFILE_UNQUALIFIED":
		case "FORBIDDEN":
		case "BUDGET_EXHAUSTED":
		case "INVALID_ARGUMENT":
			return code;
		default:
			return "FORBIDDEN";
	}
}

export function refusalToCallError(err: ControlRefused): CallError {
	switch (err.code) {
		case "IDEMPOTENCY_CONFLICT":
		case "INVALID_ARGUMENT":
		case "STALE_EXECUTION":
		case "PROFILE_UNQUALIFIED":
		case "BUDGET_EXHAUSTED":
		case "FORBIDDEN":
			return new CallError(err.code, err.message);
		case "NOT_FOUND":
			return new CallError("STALE_EXECUTION", `the execution binding names no current execution: ${err.message}`);
		case "EFFECT_UNCERTAIN":
			return new CallError("EFFECT_UNCERTAIN", err.message, true);
		default:
			return new CallError("FORBIDDEN", err.message);
	}
}
