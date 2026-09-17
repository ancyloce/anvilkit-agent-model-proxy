// The controlled call (DD-03 §6, DD-02 §4): validate the caller's binding,
// route and bounds; ask Control for the single-use permission; send exactly
// once through the ticket's transport when — and only when — this request
// was told dispatchAllowed=true; normalize the upstream into the bounded
// frame sequence; persist native evidence with its settlement, then the
// record, then the idempotent observation; answer reentries from the record
// (never a second send) and only for the call's own caller; express
// cancellation as intent and keep incurred usage; complete a settlement
// whose record write was lost from its evidence and reclaim sends this
// process lost as unknown, never as sent-nothing. Control keeps the ledger;
// this service keeps the sender's transport state.

import { ControlRefused, type DispatchPort } from "../adapters/control.js";
import { streamUpstream } from "../adapters/piai.js";
import { type CallStore, type NativeEvidence, PreconditionFailed } from "../adapters/store.js";
import { nativeUsageFromSse, SendTicket } from "../adapters/transport.js";
import type { Config, Route } from "../config.js";
import { routeDisabledReasons } from "../config.js";
import {
	type Contract,
	type ErrorCode,
	type ModelCall,
	type ModelCallRequest,
	type Outcome,
	parseStrictJson,
	type StreamFrame,
	type Usage,
} from "../contracts.js";
import {
	BoundExceeded,
	type CallRecord,
	canonicalJson,
	digestOf,
	Frames,
	isTerminal,
	type Observation,
	type Settlement,
	timestamp,
	toModelCall,
	toolSchemaDigest,
	usageFromOpenAI,
	zeroUsage,
} from "../domain/call.js";
import type { Logger } from "../log.js";

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

export interface CallDeps {
	cfg: Config;
	contract: Contract;
	control: DispatchPort;
	store: CallStore;
	log: Logger;
	/** This process's identity: the source of its observations (model-proxy/<instance>). */
	instanceId: string;
	now?: () => number;
	/** Polling interval of cancellation intents and of a send owned by another instance. */
	pollMs?: number;
}

/** An in-flight send owned by this process. */
interface LiveCall {
	callId: string;
	frames: StreamFrame[];
	subscribers: Set<(f: StreamFrame | null) => void>;
	abort: AbortController;
	cancelReason?: "cancel" | "deadline" | "bound";
	boundCode?: string;
	done: Promise<void>;
}

const sleep = (ms: number, signal?: AbortSignal) =>
	new Promise<void>((resolve) => {
		const t = setTimeout(resolve, ms);
		signal?.addEventListener("abort", () => {
			clearTimeout(t);
			resolve();
		});
	});

export class CallService {
	private readonly live = new Map<string, LiveCall>();
	private readonly retrying = new Set<string>();
	private readonly now: () => number;
	private readonly pollMs: number;
	private closing = false;
	readonly source: string;

	constructor(private readonly d: CallDeps) {
		this.now = d.now ?? Date.now;
		this.pollMs = d.pollMs ?? 500;
		this.source = `model-proxy/${d.instanceId}`.slice(0, 64);
	}

	/** In-flight sends of this process (for graceful shutdown and tests). */
	get inFlight(): number {
		return this.live.size;
	}

	private route(id: string): Route {
		const route = this.d.cfg.routes.find((r) => r.id === id);
		if (!route) throw new CallError("PROFILE_UNQUALIFIED", `route ${id} is not a reviewed route of this Proxy`);
		const reasons = routeDisabledReasons(this.d.cfg, route);
		if (reasons.length > 0)
			throw new CallError("PROFILE_UNQUALIFIED", `route ${id} is not enabled (${reasons.join(", ")})`);
		return route;
	}

	/** The digest of the immutable content of a request: what a reentry must repeat exactly. */
	private contentDigestOf(req: ModelCallRequest): string {
		return digestOf(
			canonicalJson({
				binding: req.binding,
				routeId: req.routeId,
				messages: req.messages,
				tools: req.tools ?? [],
				maxOutputTokens: req.maxOutputTokens,
				maxExposure: req.maxExposure,
				deadline: req.deadline,
				supersedesCallId: req.supersedesCallId ?? "",
				evidenceRef: req.evidenceRef ?? "",
			}),
		);
	}

	/** Validates a new call against the route profile; returns the route and the deadline. */
	private validate(req: ModelCallRequest): { route: Route; deadlineMs: number } {
		const route = this.route(req.routeId);
		const nowMs = this.now();
		const deadlineMs = Date.parse(req.deadline);
		if (Number.isNaN(deadlineMs)) throw new CallError("INVALID_ARGUMENT", "deadline is not a timestamp");
		if (deadlineMs <= nowMs) throw new CallError("STALE_EXECUTION", "the call deadline has passed");
		if (deadlineMs > nowMs + route.limits.maxDeadlineMs) {
			throw new CallError(
				"INVALID_ARGUMENT",
				`the call deadline extends the route bound of ${route.limits.maxDeadlineMs}ms`,
			);
		}
		if (req.maxOutputTokens > route.limits.maxOutputTokens) {
			throw new CallError(
				"INVALID_ARGUMENT",
				`maxOutputTokens ${req.maxOutputTokens} exceeds the route bound ${route.limits.maxOutputTokens}`,
			);
		}
		if (req.maxExposure.currency !== route.limits.maxExposure.currency) {
			throw new CallError(
				"INVALID_ARGUMENT",
				`the route is bounded in ${route.limits.maxExposure.currency}, the exposure is declared in ${req.maxExposure.currency}`,
			);
		}
		const amount = BigInt(req.maxExposure.amount);
		if (amount <= 0n) throw new CallError("INVALID_ARGUMENT", "maxExposure must be positive");
		if (amount > BigInt(route.limits.maxExposure.amount)) {
			throw new CallError(
				"INVALID_ARGUMENT",
				`maxExposure ${req.maxExposure.amount} exceeds the route bound ${route.limits.maxExposure.amount}`,
			);
		}
		const names = new Set<string>();
		for (const t of req.tools ?? []) {
			if (names.has(t.name)) throw new CallError("INVALID_ARGUMENT", `tool ${t.name} is named twice`);
			names.add(t.name);
			const schema = route.tools.find((s) => s.name === t.name);
			if (!schema)
				throw new CallError("PROFILE_UNQUALIFIED", `tool ${t.name} is not a reviewed tool of route ${route.id}`);
			if (toolSchemaDigest(schema.inputSchema) !== t.inputSchemaDigest) {
				throw new CallError(
					"PROFILE_UNQUALIFIED",
					`tool ${t.name}: the schema digest does not match the reviewed schema of route ${route.id}`,
				);
			}
		}
		this.validateToolHistory(req, route);
		return { route, deadlineMs };
	}

	/**
	 * The tool round trip of the message history: tool calls belong to
	 * assistant messages, name a reviewed tool of the route with JSON-object
	 * arguments and have distinct ids; a tool message answers exactly one
	 * earlier assistant tool call; no other role carries either.
	 */
	private validateToolHistory(req: ModelCallRequest, route: Route): void {
		const calls = new Map<string, string>();
		const answered = new Set<string>();
		req.messages.forEach((m, i) => {
			const at = `messages[${i}]`;
			if (m.role !== "assistant" && m.toolCalls !== undefined) {
				throw new CallError("INVALID_ARGUMENT", `${at}: toolCalls belong to assistant messages`);
			}
			if (m.role !== "tool" && m.toolCallId !== undefined) {
				throw new CallError("INVALID_ARGUMENT", `${at}: toolCallId belongs to tool messages`);
			}
			if (m.role === "assistant") {
				for (const tc of m.toolCalls ?? []) {
					if (calls.has(tc.toolCallId))
						throw new CallError("INVALID_ARGUMENT", `${at}: tool call ${tc.toolCallId} repeats`);
					if (!route.tools.some((s) => s.name === tc.name)) {
						throw new CallError(
							"PROFILE_UNQUALIFIED",
							`${at}: tool ${tc.name} is not a reviewed tool of route ${route.id}`,
						);
					}
					let args: unknown;
					try {
						args = parseStrictJson(tc.arguments);
					} catch (err) {
						throw new CallError("INVALID_ARGUMENT", `${at}: tool call ${tc.toolCallId}: ${(err as Error).message}`);
					}
					if (typeof args !== "object" || args === null || Array.isArray(args)) {
						throw new CallError(
							"INVALID_ARGUMENT",
							`${at}: tool call ${tc.toolCallId}: arguments are not a JSON object`,
						);
					}
					calls.set(tc.toolCallId, tc.name);
				}
			}
			if (m.role === "tool") {
				if (!m.toolCallId) throw new CallError("INVALID_ARGUMENT", `${at}: a tool message needs toolCallId`);
				if (!calls.has(m.toolCallId)) {
					throw new CallError(
						"INVALID_ARGUMENT",
						`${at}: tool call ${m.toolCallId} was not made by an earlier assistant message`,
					);
				}
				if (answered.has(m.toolCallId)) {
					throw new CallError("INVALID_ARGUMENT", `${at}: tool call ${m.toolCallId} is answered twice`);
				}
				answered.add(m.toolCallId);
			}
		});
	}

	// ---- authorization ------------------------------------------------------

	/** The call's own caller: the principal that opened it, on any of its replicas. */
	private isOwner(principal: Principal, record: CallRecord): boolean {
		return record.principalId === principal.id;
	}

	/** Who may read a record: its caller, and Control's trusted recovery queries. */
	private canRead(principal: Principal, record: CallRecord): boolean {
		return this.isOwner(principal, record) || principal.kind === "control";
	}

	/**
	 * Opens the call: a reentry of a recorded call attaches to its record (or
	 * its live send) and never sends again; a new call is admitted and, when
	 * this request holds the permission, sent once. Returns the frame stream.
	 */
	async open(principal: Principal, req: ModelCallRequest): Promise<AsyncGenerator<StreamFrame>> {
		const contentDigest = this.contentDigestOf(req);
		// A recorded call is answered from its record whatever the clock
		// says now: a reconnect or a late query keeps the original identity
		// and never opens a new send.
		const existing = await this.d.store.read(req.callId);
		if (existing) {
			this.reentry(principal, existing.record, req, contentDigest);
			return this.attach(existing.record, Date.parse(existing.record.deadline));
		}
		const { route, deadlineMs } = this.validate(req);
		const admission = await this.admit(principal, req, route);
		const nowIso = timestamp(this.now());
		const base: CallRecord = {
			callId: req.callId,
			tenantId: req.binding.tenantId,
			principalId: principal.id,
			routeId: route.id,
			provider: route.provider,
			model: route.model,
			requestDigest: req.requestDigest,
			contentDigest,
			binding: req.binding,
			deadline: req.deadline,
			maxExposure: req.maxExposure,
			maxOutputTokens: req.maxOutputTokens,
			dispatchId: admission.dispatchId,
			state: "sending",
			createdAt: nowIso,
			updatedAt: nowIso,
			revision: 1,
			frames: [],
			observations: [],
		};
		if (admission.denialCode) {
			const record = { ...base, state: "denied" as const, errorCode: admission.denialCode };
			await this.createOrAttachRecord(record);
			throw new CallError(denialErrorCode(admission.denialCode), `Control denied the call: ${admission.denialCode}`);
		}
		// The pending marker precedes the record: from the moment a record
		// that owes Control an observation exists, the sweep of any instance
		// can discover it, however early this process is lost.
		await this.d.store.markPending(req.callId, req.deadline);
		if (!admission.allowed) {
			// The permission of this call was consumed by another request: a
			// concurrent duplicate on another instance (its record appears
			// shortly) or a lost first answer (nobody can send; the outcome is
			// unknown until independently checkable evidence says otherwise).
			const record = await this.awaitRecord(req.callId, this.d.cfg.control.admissionRetry.maxIntervalMs);
			if (record) {
				this.reentry(principal, record, req, contentDigest);
				return this.attach(record, deadlineMs);
			}
			const lost = await this.createOrAttachRecord({ ...base, state: "unknown", errorCode: "PERMISSION_LOST" });
			if (lost.created) {
				lost.record.frames = this.staticFrames(req.callId, route, (f) => f.error("unknown", "PERMISSION_LOST"));
				lost.record.observations.push(this.observationOf(lost.record, "unknown", undefined, undefined));
				await this.writeRecord(lost.record, lost.version);
				this.d.log.warn("admission answered without permission and without a record: the permission was lost", {
					callId: req.callId,
					dispatchId: admission.dispatchId,
				});
				if (!(await this.submitPending(req.callId))) void this.submitObservations(req.callId);
			}
			return this.attach(lost.record, deadlineMs);
		}
		// This request holds the single permission. The record with the
		// dispatch identity is durable before any byte leaves the process: a
		// process lost after this point leaves an unknown, never a not-sent.
		const created = await this.createOrAttachRecord(base);
		let record = created.record;
		let version = created.version;
		if (!created.created) {
			// A duplicate on another instance recorded the lost permission
			// before this answer arrived; the permission holder's state wins.
			record = { ...record, ...base, revision: record.revision, createdAt: record.createdAt };
			version = await this.writeRecord(record, version);
		}
		const live = this.startSend(record, version, route, req, deadlineMs);
		return this.attachLive(live);
	}

	private reentry(principal: Principal, record: CallRecord, req: ModelCallRequest, contentDigest: string): void {
		if (!this.isOwner(principal, record)) {
			throw new CallError("FORBIDDEN", `call ${req.callId} was opened by another caller`);
		}
		if (
			record.tenantId !== req.binding.tenantId ||
			record.requestDigest !== req.requestDigest ||
			record.contentDigest !== contentDigest
		) {
			throw new CallError("IDEMPOTENCY_CONFLICT", `call ${req.callId} was recorded with other immutable content`);
		}
		if (record.state === "denied") {
			throw new CallError(
				denialErrorCode(record.errorCode ?? "FORBIDDEN"),
				`Control denied the call: ${record.errorCode}`,
			);
		}
	}

	/** AdmitModel under the call identity, reentered on a lost answer (never a second command). */
	private async admit(principal: Principal, req: ModelCallRequest, route: Route) {
		const retry = this.d.cfg.control.admissionRetry;
		let delay = retry.initialMs;
		let lastErr: unknown;
		for (let attempt = 1; attempt <= retry.maxAttempts; attempt++) {
			try {
				return await this.d.control.admitModel({
					tenantId: req.binding.tenantId,
					actorId: principal.id,
					commandId: `model-call:${req.callId}`,
					requestDigest: req.requestDigest,
					binding: req.binding,
					callId: req.callId,
					owner: this.d.cfg.identity.owner,
					routeId: route.id,
					provider: route.provider,
					model: route.model,
					maxExposure: req.maxExposure,
					// The admission deadline is the request's at millisecond
					// precision (the transport's Timestamp); callers state
					// deadlines at that precision so the identity is exact.
					deadline: new Date(req.deadline),
					supersedesCallId: req.supersedesCallId,
					evidenceRef: req.evidenceRef,
				});
			} catch (err) {
				if (err instanceof ControlRefused) throw refusalToCallError(err);
				lastErr = err;
				if (attempt < retry.maxAttempts) {
					await sleep(delay);
					delay = Math.min(delay * 2, retry.maxIntervalMs);
				}
			}
		}
		throw new CallError(
			"DEPENDENCY_UNAVAILABLE",
			`Control did not answer the admission of ${req.callId}: ${(lastErr as Error)?.message ?? lastErr}`,
			true,
		);
	}

	private async awaitRecord(callId: string, maxMs: number): Promise<CallRecord | undefined> {
		const until = this.now() + maxMs;
		for (;;) {
			const r = await this.d.store.read(callId);
			if (r) return r.record;
			if (this.now() >= until) return undefined;
			await sleep(this.pollMs);
		}
	}

	private async createOrAttachRecord(
		record: CallRecord,
	): Promise<{ record: CallRecord; version: string; created: boolean }> {
		try {
			const version = await this.d.store.create(record);
			return { record, version, created: true };
		} catch (err) {
			if (!(err instanceof PreconditionFailed)) throw err;
			const found = await this.d.store.read(record.callId);
			if (!found) throw err;
			return { record: found.record, version: found.version, created: false };
		}
	}

	private async writeRecord(record: CallRecord, version: string): Promise<string> {
		record.revision += 1;
		record.updatedAt = timestamp(this.now(), record.updatedAt);
		return this.d.store.update(record, version);
	}

	private staticFrames(callId: string, route: Route, fill: (f: Frames) => void): StreamFrame[] {
		const f = new Frames(callId, route.limits);
		f.admitted();
		fill(f);
		return f.frames;
	}

	private observationOf(
		record: CallRecord,
		outcome: Outcome,
		usage: Usage | undefined,
		nativeReference: string | undefined,
	): Observation {
		const o: Observation = {
			source: this.source,
			sequence: String(record.revision + 1),
			outcome,
			observedAt: timestamp(this.now()),
			submitted: false,
		};
		if (usage) o.usage = usage;
		if (nativeReference) o.nativeReference = nativeReference;
		return o;
	}

	// ---- the single send -------------------------------------------------

	private startSend(
		record: CallRecord,
		version: string,
		route: Route,
		req: ModelCallRequest,
		deadlineMs: number,
	): LiveCall {
		const abort = new AbortController();
		const live: LiveCall = {
			callId: record.callId,
			frames: [],
			subscribers: new Set(),
			abort,
			done: Promise.resolve(),
		};
		this.live.set(record.callId, live);
		live.done = this.runSend(live, record, version, route, req, deadlineMs)
			.catch((err) => {
				this.d.log.error("send failed outside the protocol path", { callId: record.callId, error: String(err) });
			})
			.finally(() => {
				this.live.delete(record.callId);
				this.broadcast(live, null);
			});
		return live;
	}

	private broadcast(live: LiveCall, frame: StreamFrame | null): void {
		if (frame) live.frames.push(frame);
		for (const s of live.subscribers) s(frame);
	}

	private async runSend(
		live: LiveCall,
		record: CallRecord,
		_version: string,
		route: Route,
		req: ModelCallRequest,
		deadlineMs: number,
	): Promise<void> {
		const startedAt = this.now();
		const credential = this.d.cfg.credentials.get(route.id) ?? "";
		const ticket = new SendTicket(record.callId, route.baseUrl, credential, route.limits.maxEvidenceBytes);
		// The final frame reaches the callers only once the evidence and the
		// record are durable and the observation was offered to Control: a
		// query right after the stream ended answers the same outcome.
		const frames = new Frames(record.callId, route.limits, (f) => {
			if (f.type !== "done" && f.type !== "error") this.broadcast(live, f);
		});
		frames.admitted();
		const remaining = Math.max(0, deadlineMs - this.now());
		const timeoutMs = Math.min(remaining, route.limits.upstreamTimeoutMs);
		const deadlineTimer = setTimeout(() => {
			live.cancelReason ??= "deadline";
			live.abort.abort();
		}, remaining);
		// Cancellation is durable intent: a cancel that reached another
		// instance is seen by the sender through the store.
		const cancelPoll = setInterval(() => {
			void this.d.store.cancelRequested(record.callId).then((at) => {
				if (at && !live.abort.signal.aborted) {
					live.cancelReason ??= "cancel";
					live.abort.abort();
				}
			});
		}, this.pollMs);
		let upstreamError: string | undefined;
		try {
			// A cancel that reached the store before the first byte leaves is
			// honored without a send.
			if (!live.abort.signal.aborted && (await this.d.store.cancelRequested(record.callId))) {
				live.cancelReason ??= "cancel";
				live.abort.abort();
			}
			if (live.abort.signal.aborted) throw new Error("aborted before the send");
			for await (const ev of streamUpstream({
				route,
				credential,
				messages: req.messages,
				tools: req.tools ?? [],
				maxOutputTokens: req.maxOutputTokens,
				timeoutMs,
				signal: live.abort.signal,
				ticket,
			})) {
				try {
					switch (ev.type) {
						case "text":
							frames.text(ev.delta);
							break;
						case "tool_call":
							frames.toolCall(ev.id, ev.name, ev.arguments);
							break;
						case "done":
							break;
						case "error":
							upstreamError = ev.message;
							break;
					}
				} catch (err) {
					if (err instanceof BoundExceeded) {
						live.cancelReason ??= "bound";
						live.boundCode = err.code;
						live.abort.abort();
						break;
					}
					throw err;
				}
			}
		} catch (err) {
			upstreamError = err instanceof Error ? err.message : String(err);
		} finally {
			clearTimeout(deadlineTimer);
			clearInterval(cancelPoll);
		}
		// The evidence branch of the transport finishes with the response.
		await Promise.race([ticket.settled, sleep(5_000)]);
		const settlement = this.settle(live, ticket, upstreamError, frames, record.callId);
		const evidence: NativeEvidence = {
			callId: record.callId,
			dispatchId: record.dispatchId,
			routeId: route.id,
			capturedAt: timestamp(this.now()),
			request: ticket.request ?? { method: "", url: "", contentType: "", body: "", bodyTruncated: false },
			settlement,
		};
		if (ticket.response) {
			evidence.response = {
				status: ticket.response.status,
				headers: ticket.response.headers,
				bodyBase64: Buffer.from(ticket.response.body).toString("base64"),
				bodyBytes: ticket.response.bodyBytes,
				bodyTruncated: ticket.response.bodyTruncated,
			};
		}
		if (ticket.transportError) evidence.transportError = ticket.transportError;
		this.d.log.info("call finished", {
			callId: record.callId,
			dispatchId: record.dispatchId,
			routeId: route.id,
			outcome: settlement.outcome,
			errorCode: settlement.errorCode,
			frames: frames.count,
			upstreamStatus: ticket.response?.status,
			refusedSends: ticket.refused.length,
			durationMs: this.now() - startedAt,
		});
		// Durable order: the evidence with its settlement, then the record with
		// the pending observation, then the observation itself. Nothing is
		// reported that is not recorded, and nothing unrecorded is handed to
		// the callers: while the store does not take the settlement, the
		// callers wait (their own bounds end the wait; a reentry finds the
		// record still sending) and the settlement is retried here; lost with
		// this process, the evidence — once written — lets the sweep complete
		// it, and without evidence the sweep reclaims the send as unknown.
		let delay = this.d.cfg.observation.retryInitialMs;
		for (;;) {
			try {
				const evidenceRef = await this.d.store.writeEvidence(evidence);
				await this.publish(record.callId, settlement, evidenceRef);
				break;
			} catch (err) {
				this.d.log.error("settlement not persisted; retrying, the callers wait", {
					callId: record.callId,
					dispatchId: record.dispatchId,
					error: String(err),
				});
				if (this.closing) return;
				await sleep(delay);
				delay = Math.min(delay * 2, this.d.cfg.observation.retryMaxIntervalMs);
			}
		}
		// The observation is offered to Control once before the caller's
		// stream ends (the ledger sees the outcome the caller is handed);
		// an unavailable Control is retried in the background under the same
		// identity, and by the sweep after a restart.
		if (!(await this.submitPending(record.callId))) void this.submitObservations(record.callId);
		const last = settlement.frames.at(-1);
		if (last && (last.type === "done" || last.type === "error")) this.broadcast(live, last);
	}

	/**
	 * What the send established, from the ticket's facts and the native
	 * counters the response carried: the outcome, the error code, the usage
	 * (only complete, validated counters), the native reference and the
	 * frames ending with the one final frame.
	 */
	private settle(
		live: LiveCall,
		ticket: SendTicket,
		upstreamError: string | undefined,
		frames: Frames,
		callId: string,
	): Settlement {
		const native = ticket.response ? nativeUsageFromSse(ticket.response.body) : undefined;
		const report = native ? usageFromOpenAI(native.usage) : undefined;
		const usage = report?.kind === "complete" ? report.usage : undefined;
		let nativeReference: string | undefined;
		if (native?.responseId) nativeReference = native.responseId;
		else if (ticket.response?.headers["x-request-id"]) nativeReference = ticket.response.headers["x-request-id"];
		const okStatus = ticket.response !== undefined && ticket.response.status >= 200 && ticket.response.status < 300;
		let outcome: Outcome;
		let errorCode: string | undefined;
		let settledUsage = usage;
		if (
			!ticket.used ||
			(ticket.transportError !== undefined && !ticket.response && /ECONNREFUSED/.test(ticket.transportError))
		) {
			// Nothing left this process (no permission was exercised on the
			// wire, or the connection was refused before a byte was sent): a
			// certain fact of the sender, settled with explicit zero usage.
			outcome = live.cancelReason === "cancel" ? "canceled" : "failed";
			errorCode = live.cancelReason === "cancel" ? "CANCELED_BEFORE_SEND" : "UPSTREAM_UNREACHABLE";
			settledUsage = zeroUsage;
		} else if (live.cancelReason === "cancel") {
			outcome = usage ? "canceled" : "unknown";
			errorCode = "CANCELED";
		} else if (live.cancelReason === "deadline") {
			outcome = usage ? "failed" : "unknown";
			errorCode = "DEADLINE_EXCEEDED";
		} else if (live.cancelReason === "bound") {
			outcome = usage ? "failed" : "unknown";
			errorCode = live.boundCode;
		} else if (upstreamError !== undefined || !okStatus) {
			outcome = usage ? "failed" : "unknown";
			errorCode = ticket.response && !okStatus ? `UPSTREAM_STATUS_${ticket.response.status}` : "UPSTREAM_ERROR";
		} else if (!report) {
			// The stream completed but no chunk carried usage: the text is
			// delivered, the cost is unknown, never zero.
			outcome = "unknown";
			errorCode = "USAGE_MISSING";
		} else if (report.kind === "invalid") {
			// A usage object outside the protocol establishes no counters: the
			// evidence keeps what was reported, the exposure stays retained.
			outcome = "unknown";
			errorCode = "USAGE_INVALID";
			this.d.log.warn("native usage outside the protocol; the outcome stays unknown", {
				callId,
				reason: report.reason,
			});
		} else {
			outcome = "succeeded";
		}
		try {
			if (outcome === "succeeded" && settledUsage) {
				frames.usage(settledUsage);
				frames.done(settledUsage);
			} else {
				frames.error(outcome as Exclude<Outcome, "succeeded">, errorCode ?? "UPSTREAM_ERROR", settledUsage);
			}
		} catch (err) {
			// The final frame has a reserved slot and a bounded size; failing to
			// place it is a defect of this service, never a lost outcome.
			this.d.log.error("final frame refused by the bounds", { callId, error: String(err) });
		}
		const settlement: Settlement = { outcome, frames: frames.frames };
		if (errorCode) settlement.errorCode = errorCode;
		if (settledUsage) settlement.usage = settledUsage;
		if (nativeReference) settlement.nativeReference = nativeReference;
		return settlement;
	}

	/**
	 * Publishes a settlement on the record under conditional writes: a record
	 * moved by a concurrent cancel or by another instance is reloaded and the
	 * settlement applied again; a record already carrying this settlement is
	 * left as it is; a record the sweep reclaimed as unknown before the
	 * sender's settlement landed takes the settlement when it establishes
	 * more (a definite outcome, or usage), with a supplemental observation
	 * under a fresh sequence — Control corrects its ledger from it and never
	 * charges the same cumulative usage twice.
	 */
	private async publish(callId: string, settlement: Settlement, evidenceRef: string): Promise<void> {
		for (;;) {
			const found = await this.d.store.read(callId);
			if (!found) throw new Error(`call ${callId} has no record to settle`);
			const rec = found.record;
			if (rec.evidenceRef === evidenceRef && rec.state === settlement.outcome) return;
			let observe = true;
			if (isTerminal(rec.state)) {
				if (rec.state !== "unknown") return; // settled by an earlier publication of the same evidence
				// Reclaimed as unknown: the settlement replaces it only with more evidence.
				if (settlement.outcome === "unknown" && !settlement.usage) observe = false;
				else {
					this.d.log.warn("the sender's settlement supersedes the reclaimed unknown outcome", {
						callId,
						dispatchId: rec.dispatchId,
						outcome: settlement.outcome,
					});
				}
			}
			rec.state = settlement.outcome;
			if (settlement.errorCode) rec.errorCode = settlement.errorCode;
			else delete rec.errorCode;
			if (settlement.usage) rec.usage = settlement.usage;
			else delete rec.usage;
			if (settlement.nativeReference) rec.nativeReference = settlement.nativeReference;
			else delete rec.nativeReference;
			rec.evidenceRef = evidenceRef;
			rec.frames = settlement.frames;
			if (observe) {
				rec.observations.push(
					this.observationOf(rec, settlement.outcome, settlement.usage, settlement.nativeReference),
				);
			}
			try {
				await this.writeRecord(rec, found.version);
				return;
			} catch (err) {
				if (!(err instanceof PreconditionFailed)) throw err;
				this.d.log.info("record moved during settlement; reapplying", { callId });
			}
		}
	}

	// ---- attaching callers ------------------------------------------------

	private attachLive(live: LiveCall): AsyncGenerator<StreamFrame> {
		const queue: (StreamFrame | null)[] = [...live.frames];
		let wake: (() => void) | undefined;
		const push = (f: StreamFrame | null) => {
			queue.push(f);
			wake?.();
		};
		live.subscribers.add(push);
		const closedAlready = !this.live.has(live.callId);
		if (closedAlready) queue.push(null);
		return (async function* () {
			try {
				for (;;) {
					if (queue.length === 0) {
						await new Promise<void>((r) => {
							wake = r;
						});
						wake = undefined;
						continue;
					}
					const f = queue.shift();
					if (f === null || f === undefined) return;
					yield f;
				}
			} finally {
				live.subscribers.delete(push);
			}
		})();
	}

	/** Replays a recorded call; a send owned elsewhere is awaited through the store until it is terminal or reclaimable. */
	private attach(record: CallRecord, deadlineMs: number): AsyncGenerator<StreamFrame> {
		const live = this.live.get(record.callId);
		if (live) return this.attachLive(live);
		const self = this;
		return (async function* () {
			let current = record;
			const until = deadlineMs + self.d.cfg.observation.reclaimGraceMs + 5_000;
			while (!isTerminal(current.state)) {
				if (self.now() > until) break;
				await sleep(self.pollMs);
				const again = await self.d.store.read(current.callId);
				if (!again) break;
				current = again.record;
				const nowLive = self.live.get(current.callId);
				if (nowLive) {
					yield* self.attachLive(nowLive);
					return;
				}
			}
			for (const f of current.frames) yield f;
		})();
	}

	// ---- queries and cancellation ------------------------------------------

	/** The record's public view, for its caller or a trusted Control query; nobody else learns whether the call exists. */
	async get(principal: Principal, callId: string): Promise<ModelCall> {
		const found = await this.d.store.read(callId);
		if (!found || !this.canRead(principal, found.record)) throw new CallError("NOT_FOUND", `no call ${callId}`);
		return toModelCall(found.record);
	}

	/** Records the caller's cancel intent durably and aborts a send this process owns; incurred usage stays. */
	async cancel(principal: Principal, callId: string): Promise<ModelCall> {
		const found = await this.d.store.read(callId);
		if (!found || !this.isOwner(principal, found.record)) throw new CallError("NOT_FOUND", `no call ${callId}`);
		const at = timestamp(this.now());
		await this.d.store.requestCancel(callId, at);
		const live = this.live.get(callId);
		if (live && !live.abort.signal.aborted) {
			live.cancelReason ??= "cancel";
			live.abort.abort();
		}
		const rec = found.record;
		if (!rec.cancelRequestedAt && !isTerminal(rec.state)) {
			rec.cancelRequestedAt = at;
			try {
				await this.writeRecord(rec, found.version);
			} catch (err) {
				if (!(err instanceof PreconditionFailed)) throw err;
			}
		}
		const after = await this.d.store.read(callId);
		return toModelCall(after?.record ?? rec);
	}

	// ---- observations, retries and the sweep ----------------------------------

	/**
	 * One pass over the unsubmitted observations of the call, each under its
	 * recorded source and sequence. Returns true when nothing is pending
	 * afterwards (the marker is cleared), false when Control did not answer.
	 */
	async submitPending(callId: string): Promise<boolean> {
		for (;;) {
			const found = await this.d.store.read(callId);
			if (!found) return true;
			const pending = found.record.observations.filter((o) => !o.submitted);
			if (pending.length === 0) {
				await this.d.store.clearPending(callId);
				return true;
			}
			let failed: unknown;
			for (const o of pending) {
				try {
					const res = await this.d.control.observe({
						dispatchId: found.record.dispatchId,
						source: o.source,
						sequence: o.sequence,
						outcome: o.outcome,
						usage: o.usage,
						nativeReference: o.nativeReference,
						observedAt: new Date(o.observedAt),
					});
					o.submitted = true;
					this.d.log.info("observation recorded", {
						callId,
						dispatchId: found.record.dispatchId,
						source: o.source,
						sequence: o.sequence,
						outcome: o.outcome,
						existing: res.existing,
						dispatchState: res.state,
					});
				} catch (err) {
					if (err instanceof ControlRefused) {
						// Control refused the report on its rules (for example a
						// denied dispatch): recorded as submitted so it is not
						// repeated; the refusal is diagnostic.
						o.submitted = true;
						this.d.log.warn("observation refused by Control", {
							callId,
							dispatchId: found.record.dispatchId,
							code: err.code,
							sequence: o.sequence,
						});
						continue;
					}
					failed = err;
					break;
				}
			}
			try {
				await this.writeRecord(found.record, found.version);
			} catch (err) {
				if (!(err instanceof PreconditionFailed)) throw err;
				continue; // the record moved; reload and reconcile what is still pending
			}
			if (failed) {
				this.d.log.warn("observation not answered; it stays pending under the same identity", {
					callId,
					error: String(failed),
				});
				return false;
			}
		}
	}

	/** Retries submitPending with backoff until nothing is pending or the service closes. */
	async submitObservations(callId: string): Promise<void> {
		if (this.retrying.has(callId)) return;
		this.retrying.add(callId);
		try {
			let delay = this.d.cfg.observation.retryInitialMs;
			while (!(await this.submitPending(callId))) {
				if (this.closing) return;
				await sleep(delay);
				delay = Math.min(delay * 2, this.d.cfg.observation.retryMaxIntervalMs);
			}
		} finally {
			this.retrying.delete(callId);
		}
	}

	/**
	 * The sweep over the pending markers: resubmits pending observations
	 * (idempotent by identity); completes a send still recorded as sending
	 * whose evidence carries its settlement (the sender persisted the
	 * evidence and was lost before the record took it — the original
	 * settlement, never a resend); reclaims a send whose deadline plus grace
	 * passed without evidence as unknown — the permission was consumed,
	 * whether bytes left is not known, so the exposure stays until
	 * independently checkable evidence resolves it. A marker without a record
	 * is cleared once the call's deadline plus grace passed (its opener wrote
	 * the marker first and died before the record). A send this process owns
	 * is never touched.
	 */
	async sweep(): Promise<{ resubmitted: number; reclaimed: number; completed: number }> {
		let resubmitted = 0;
		let reclaimed = 0;
		let completed = 0;
		const grace = this.d.cfg.observation.reclaimGraceMs;
		for (const callId of await this.d.store.listPending()) {
			if (this.live.has(callId)) continue;
			const found = await this.d.store.read(callId);
			if (!found) {
				const marker = await this.d.store.readPending(callId);
				const deadlineMs = marker?.deadline ? Date.parse(marker.deadline) : Number.NaN;
				if (Number.isNaN(deadlineMs) || this.now() > deadlineMs + grace) await this.d.store.clearPending(callId);
				continue;
			}
			let rec = found.record;
			if (rec.state === "sending") {
				const evidence = await this.d.store.readEvidence(callId);
				if (evidence?.settlement) {
					await this.publish(callId, evidence.settlement, `evidence/${callId}`);
					this.d.log.warn("settlement completed from the evidence: the sender did not finish its record", {
						callId,
						dispatchId: rec.dispatchId,
						outcome: evidence.settlement.outcome,
					});
					completed++;
				} else {
					const deadlineMs = Date.parse(rec.deadline);
					if (this.now() <= deadlineMs + grace) continue;
					rec.state = "unknown";
					rec.errorCode = "SENDER_LOST";
					const route = this.d.cfg.routes.find((r) => r.id === rec.routeId);
					rec.frames = this.staticFrames(
						rec.callId,
						route ?? ({ limits: { maxFrameBytes: 65536, maxOutputBytes: 1, maxFrames: 2 } } as Route),
						(f) => f.error("unknown", "SENDER_LOST"),
					);
					rec.observations.push(this.observationOf(rec, "unknown", undefined, undefined));
					try {
						await this.writeRecord(rec, found.version);
					} catch (err) {
						if (err instanceof PreconditionFailed) continue;
						throw err;
					}
					this.d.log.warn("send reclaimed as unknown: the sender did not finish before the deadline", {
						callId,
						dispatchId: rec.dispatchId,
					});
					reclaimed++;
				}
				const again = await this.d.store.read(callId);
				if (!again) continue;
				rec = again.record;
			}
			if (rec.observations.some((o) => !o.submitted)) {
				resubmitted++;
				void this.submitObservations(callId);
			} else {
				await this.d.store.clearPending(callId);
			}
		}
		return { resubmitted, reclaimed, completed };
	}

	/** Stops retry loops; in-flight sends run to their end (bounded by their deadlines). */
	async close(): Promise<void> {
		this.closing = true;
		await Promise.all([...this.live.values()].map((l) => l.done));
	}
}

function denialErrorCode(code: string): ErrorCode {
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

function refusalToCallError(err: ControlRefused): CallError {
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
