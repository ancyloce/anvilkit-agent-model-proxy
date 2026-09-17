// The call domain (DD-03 §6, DD-02 §4 step 5): the durable record of one
// controlled call, its state machine, the bounded frame sequence of the
// StreamFrame contract, the digests the Proxy binds and the classification
// of native usage. No I/O here.
import { createHash } from "node:crypto";
import type { ExecutionBinding, ModelCall, ModelCallState, Outcome, StreamFrame, Usage } from "../contracts.js";

/** sha256 of bytes as the contract Digest ("sha256:" + 64 hex). */
export function digestOf(data: string | Uint8Array): string {
	return `sha256:${createHash("sha256").update(data).digest("hex")}`;
}

/**
 * Canonical JSON (members sorted by key at every level, no whitespace,
 * UTF-8): the digest input of tool schemas and of the immutable request
 * content. Go reproduces it with a sorted marshal of the same value.
 */
export function canonicalJson(value: unknown): string {
	if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
	if (value !== null && typeof value === "object") {
		const keys = Object.keys(value as Record<string, unknown>).sort();
		return `{${keys.map((k) => `${JSON.stringify(k)}:${canonicalJson((value as Record<string, unknown>)[k])}`).join(",")}}`;
	}
	return JSON.stringify(value);
}

export function toolSchemaDigest(inputSchema: Record<string, unknown>): string {
	return digestOf(canonicalJson(inputSchema));
}

export type CallState = ModelCallState;

/** One outcome/usage observation the Proxy owes Control (idempotent by source and sequence). */
export interface Observation {
	source: string;
	sequence: string;
	outcome: Outcome;
	usage?: Usage;
	nativeReference?: string;
	observedAt: string;
	submitted: boolean;
}

/** The durable call record (the Proxy's transport state; Control keeps the ledger). */
export interface CallRecord {
	callId: string;
	tenantId: string;
	principalId: string;
	routeId: string;
	provider: string;
	model: string;
	requestDigest: string;
	contentDigest: string;
	binding: ExecutionBinding;
	deadline: string;
	maxExposure: { currency: string; amount: string };
	maxOutputTokens: number;
	dispatchId: string;
	state: CallState;
	errorCode?: string;
	usage?: Usage;
	nativeReference?: string;
	evidenceRef?: string;
	createdAt: string;
	updatedAt: string;
	revision: number;
	/** The normalized frames of a completed send, replayed to a reentering caller. */
	frames: StreamFrame[];
	observations: Observation[];
	cancelRequestedAt?: string;
}

/**
 * The settlement of one send: what its evidence establishes and what the
 * record publishes. Persisted beside the native evidence so a record write
 * lost with its process is completed later from the evidence, not
 * reclaimed as sender-lost.
 */
export interface Settlement {
	outcome: Outcome;
	errorCode?: string;
	usage?: Usage;
	nativeReference?: string;
	frames: StreamFrame[];
}

export const terminalStates: ReadonlySet<CallState> = new Set(["denied", "succeeded", "failed", "canceled", "unknown"]);

export function isTerminal(state: CallState): boolean {
	return terminalStates.has(state);
}

/** The public view of a record (the ModelCall schema; frames, evidence and observations stay private). */
export function toModelCall(r: CallRecord): ModelCall {
	const out: ModelCall = {
		callId: r.callId,
		routeId: r.routeId,
		state: r.state,
		dispatchId: r.dispatchId,
		createdAt: r.createdAt,
		updatedAt: r.updatedAt,
	};
	if (r.usage) out.usage = r.usage;
	if (r.nativeReference) out.nativeReference = r.nativeReference;
	if (r.errorCode) out.errorCode = r.errorCode;
	return out;
}

/** Contract timestamps with microsecond precision, strictly increasing per record. */
export function timestamp(nowMs: number, after?: string): string {
	let micros = BigInt(Math.floor(nowMs)) * 1000n;
	if (after) {
		const prev = parseTimestampMicros(after);
		if (prev !== undefined && micros <= prev) micros = prev + 1n;
	}
	const ms = micros / 1000n;
	const frac = micros % 1000n;
	const iso = new Date(Number(ms)).toISOString(); // ...sss Z
	return `${iso.slice(0, -1)}${frac.toString().padStart(3, "0")}Z`;
}

export function parseTimestampMicros(ts: string): bigint | undefined {
	const m = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$/.exec(ts);
	if (!m) return undefined;
	const ms = Date.parse(`${m[1]}Z`);
	if (Number.isNaN(ms)) return undefined;
	const frac = (m[2] ?? "").padEnd(6, "0").slice(0, 6);
	return BigInt(ms) * 1000n + BigInt(frac);
}

export class BoundExceeded extends Error {
	constructor(readonly code: "FRAME_BOUND_EXCEEDED" | "OUTPUT_BOUND_EXCEEDED" | "SEQUENCE_BOUND_EXCEEDED") {
		super(code);
	}
}

export interface FrameBounds {
	/** The largest encoded frame (UTF-8 bytes of the frame's JSON) a caller receives. */
	maxFrameBytes: number;
	/** The total content (UTF-8 bytes of text and tool arguments) one call may deliver. */
	maxOutputBytes: number;
	/** The frame count of one call, the final frame included. */
	maxFrames: number;
}

/** The contract's own bounds on frame members (openapi/model-proxy.yaml StreamFrame), in bytes: the encoded form never exceeds them. */
export const contractTextBytes = 65536;
export const contractToolArgumentBytes = 262144;

export function utf8Bytes(text: string): number {
	return Buffer.byteLength(text, "utf8");
}

/** The UTF-8 size of a frame as it is encoded into the SSE data payload. */
export function encodedFrameBytes(frame: StreamFrame): number {
	return utf8Bytes(JSON.stringify(frame));
}

/** The bytes one code point occupies inside a JSON string (JSON.stringify escaping; raw UTF-8 otherwise). */
function jsonStringBytes(codePoint: number): number {
	if (codePoint === 0x22 || codePoint === 0x5c) return 2; // \" and \\
	if (codePoint < 0x20) {
		return codePoint === 0x08 || codePoint === 0x09 || codePoint === 0x0a || codePoint === 0x0c || codePoint === 0x0d
			? 2
			: 6;
	}
	if (codePoint >= 0xd800 && codePoint <= 0xdfff) return 6; // a lone surrogate is written as \uXXXX
	if (codePoint < 0x80) return 1;
	if (codePoint < 0x800) return 2;
	if (codePoint < 0x10000) return 3;
	return 4;
}

/**
 * Frames builds the bounded, monotonic frame sequence of one call: sequence
 * 0 is `admitted`; text is split at the route's encoded frame bound (and the
 * contract's text bound) without breaking a code point; a tool call is one
 * frame within the same bound and the contract's argument bound; the total
 * content and the frame count are bounded by the route profile; the last
 * slot of the count is reserved for the final frame, so the outcome of a
 * bound hit — `done` or `error` exactly once — always reaches the caller.
 */
export class Frames {
	readonly frames: StreamFrame[] = [];
	private outputBytes = 0;
	private closed = false;
	private readonly textBudget: number;

	constructor(
		readonly callId: string,
		readonly bounds: FrameBounds,
		private readonly sink?: (f: StreamFrame) => void,
	) {
		const envelope = encodedFrameBytes({
			callId,
			sequence: "9".repeat(String(bounds.maxFrames).length),
			type: "text",
			text: "",
		});
		this.textBudget = Math.min(contractTextBytes, bounds.maxFrameBytes - envelope);
	}

	get count(): number {
		return this.frames.length;
	}

	get isClosed(): boolean {
		return this.closed;
	}

	/** Content frames may take every slot but the last; the final frame takes the last one. */
	private hasRoom(final: boolean): boolean {
		return this.frames.length < (final ? this.bounds.maxFrames : this.bounds.maxFrames - 1);
	}

	private push(frame: Omit<StreamFrame, "callId" | "sequence">, final = false): StreamFrame {
		if (this.closed) throw new Error("frame after the final frame");
		if (!this.hasRoom(final)) throw new BoundExceeded("SEQUENCE_BOUND_EXCEEDED");
		const full: StreamFrame = { callId: this.callId, sequence: String(this.frames.length), ...frame };
		// A final frame is bounded by the contract itself (ids, counters and the
		// error code have fixed maxima, under 512 bytes together), never by the
		// route: the outcome always fits.
		if (!final && encodedFrameBytes(full) > this.bounds.maxFrameBytes) throw new BoundExceeded("FRAME_BOUND_EXCEEDED");
		this.frames.push(full);
		this.sink?.(full);
		return full;
	}

	admitted(): void {
		this.push({ type: "admitted" });
	}

	/** Appends text, split into frames whose encoded size stays within the bounds; a code point is never split. */
	text(delta: string): void {
		if (this.textBudget <= 0) throw new BoundExceeded("FRAME_BOUND_EXCEEDED");
		let chunk = "";
		let bytes = 0;
		for (const ch of delta) {
			const b = jsonStringBytes(ch.codePointAt(0) ?? 0);
			if (bytes + b > this.textBudget && chunk.length > 0) {
				this.emitText(chunk);
				chunk = "";
				bytes = 0;
			}
			chunk += ch;
			bytes += b;
		}
		if (chunk.length > 0) this.emitText(chunk);
	}

	private emitText(chunk: string): void {
		this.account(utf8Bytes(chunk));
		this.push({ type: "text", text: chunk });
	}

	toolCall(toolCallId: string, name: string, args: string): void {
		const bytes = utf8Bytes(args);
		if (bytes > contractToolArgumentBytes) throw new BoundExceeded("FRAME_BOUND_EXCEEDED");
		this.account(bytes);
		this.push({ type: "tool_call", toolCall: { toolCallId, name, argumentsDigest: digestOf(args), arguments: args } });
	}

	/** The usage frame is informational (the final frame carries the usage as well): skipped when no content slot is left. */
	usage(usage: Usage): void {
		if (!this.hasRoom(false)) return;
		this.push({ type: "usage", usage });
	}

	done(usage: Usage): void {
		this.push({ type: "done", outcome: "succeeded", usage }, true);
		this.closed = true;
	}

	error(outcome: Exclude<Outcome, "succeeded">, errorCode: string, usage?: Usage): void {
		const frame: Omit<StreamFrame, "callId" | "sequence"> = {
			type: "error",
			outcome,
			errorCode: errorCode.slice(0, 64),
		};
		if (usage) frame.usage = usage;
		this.push(frame, true);
		this.closed = true;
	}

	private account(bytes: number): void {
		if (this.closed) throw new Error("frame after the final frame");
		if (this.outputBytes + bytes > this.bounds.maxOutputBytes) throw new BoundExceeded("OUTPUT_BOUND_EXCEEDED");
		this.outputBytes += bytes;
	}
}

/** What a native usage object establishes: complete counters, or a report that cannot settle anything. */
export type NativeUsageReport = { kind: "complete"; usage: Usage } | { kind: "invalid"; reason: string };

function isObject(v: unknown): v is Record<string, unknown> {
	return typeof v === "object" && v !== null && !Array.isArray(v);
}

function counter(v: unknown): number | undefined {
	return typeof v === "number" && Number.isSafeInteger(v) && v >= 0 ? v : undefined;
}

/**
 * Native usage counters of the openai-completions protocol as the provider
 * reported them, validated against that protocol before they can settle
 * anything: `prompt_tokens` and `completion_tokens` are required
 * non-negative integers; `total_tokens`, when present, is their sum;
 * `prompt_tokens_details.cached_tokens` and
 * `completion_tokens_details.reasoning_tokens` are optional, count as zero
 * when absent (the protocol reports them only where they apply) and are
 * subsets of the prompt and completion counters when present. Anything else
 * — an empty object, a missing or negative or fractional counter, a detail
 * that is not an object — is insufficient evidence: the outcome stays
 * unknown with the exposure retained, never zero cost. The counters are
 * reported inclusive as the protocol states them; Control's price book
 * interprets inclusion (DD-02 §3).
 */
export function usageFromOpenAI(raw: unknown): NativeUsageReport {
	if (!isObject(raw)) return { kind: "invalid", reason: "usage is not an object" };
	const prompt = counter(raw.prompt_tokens);
	if (prompt === undefined) return { kind: "invalid", reason: "prompt_tokens is not a non-negative integer" };
	const completion = counter(raw.completion_tokens);
	if (completion === undefined) return { kind: "invalid", reason: "completion_tokens is not a non-negative integer" };
	if (raw.total_tokens !== undefined && raw.total_tokens !== null) {
		const total = counter(raw.total_tokens);
		if (total === undefined || total !== prompt + completion) {
			return { kind: "invalid", reason: "total_tokens is not the sum of prompt_tokens and completion_tokens" };
		}
	}
	let cached = 0;
	if (raw.prompt_tokens_details !== undefined && raw.prompt_tokens_details !== null) {
		if (!isObject(raw.prompt_tokens_details))
			return { kind: "invalid", reason: "prompt_tokens_details is not an object" };
		const v = raw.prompt_tokens_details.cached_tokens;
		if (v !== undefined && v !== null) {
			const n = counter(v);
			if (n === undefined || n > prompt) {
				return {
					kind: "invalid",
					reason: "prompt_tokens_details.cached_tokens is not an integer within prompt_tokens",
				};
			}
			cached = n;
		}
	}
	let reasoning = 0;
	if (raw.completion_tokens_details !== undefined && raw.completion_tokens_details !== null) {
		if (!isObject(raw.completion_tokens_details)) {
			return { kind: "invalid", reason: "completion_tokens_details is not an object" };
		}
		const v = raw.completion_tokens_details.reasoning_tokens;
		if (v !== undefined && v !== null) {
			const n = counter(v);
			if (n === undefined || n > completion) {
				return {
					kind: "invalid",
					reason: "completion_tokens_details.reasoning_tokens is not an integer within completion_tokens",
				};
			}
			reasoning = n;
		}
	}
	return {
		kind: "complete",
		usage: {
			inputUnits: String(prompt),
			outputUnits: String(completion),
			reasoningUnits: String(reasoning),
			cachedInputUnits: String(cached),
		},
	};
}

export const zeroUsage: Usage = { inputUnits: "0", outputUnits: "0", reasoningUnits: "0", cachedInputUnits: "0" };
