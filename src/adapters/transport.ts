// The guarded transport (DD-02 §4 step 5, DD-03 §6): the only outbound HTTP
// path of the process. pi-ai 1.1.0 takes the fetch implementation of a
// call as a request option (ProviderRequestOptions.fetch, handed to the
// vendor client it builds per call), so every send is made through the
// fetch of one SendTicket — the consumed single-use permission of one call.
//
// The ticket's fetch is not the platform fetch. The platform fetch re-sends a
// request beneath its caller on HTTP 421 (and follows redirects unless told
// not to), so one admitted send could reach the wire twice while the ticket
// counted one (review finding B-17). The ticket therefore drives the wire
// itself: it owns a per-ticket undici Dispatcher whose `dispatch()` permits
// exactly one call — a second attempt, whatever layer makes it, is refused
// with a recorded `SEND_ALREADY_ATTEMPTED` and never reaches the wire — and
// builds the Response the vendor client reads from that one dispatch's
// handler callbacks. There is no retry, redirect or fallback layer between
// the ticket and the connection pool.
//
// The request may only reach the route's configured origin and path, a 3xx
// is a failed send (never followed), the Authorization header must be
// exactly the route credential the Proxy attached (an inbound AnvilKit token
// can never be forwarded), and the request payload and the raw response are
// captured as private native evidence together with the wire attempt count.
// The process-wide fetch is replaced at bootstrap by one that refuses:
// nothing in this process sends without a ticket, hence without an
// admission behind it.
import { createParser } from "eventsource-parser";

export interface CapturedRequest {
	method: string;
	url: string;
	contentType: string;
	body: string;
	bodyTruncated: boolean;
	/** Bytes of the request body handed to the one dispatch. */
	bodyBytes: number;
}

export interface CapturedResponse {
	status: number;
	headers: Record<string, string>;
	body: Uint8Array;
	bodyBytes: number;
	bodyTruncated: boolean;
}

const evidenceHeaders = ["content-type", "date", "x-request-id", "retry-after"];

export class TransportRefused extends Error {}

/** Recorded on the ticket when a second dispatch was attempted and refused. */
export const SEND_ALREADY_ATTEMPTED = "SEND_ALREADY_ATTEMPTED";

// undici's dispatch surface, as Node's bundled Agent exposes it (undici 7
// controller-style handler). Typed locally: the module is not importable
// from the platform, only its global dispatcher instance is.
export interface DispatchOptions {
	origin: string;
	path: string;
	method: string;
	headers?: Record<string, string>;
	body?: string | Uint8Array | null;
	[key: string]: unknown;
}

export interface DispatchController {
	abort(reason?: unknown): void;
	pause(): void;
	resume(): void;
	readonly aborted?: boolean;
	readonly paused?: boolean;
}

export interface DispatchHandler {
	onRequestStart?(controller: DispatchController, context: unknown): void;
	onRequestUpgrade?(controller: DispatchController, statusCode: number, headers: unknown, socket: unknown): void;
	onResponseStart?(
		controller: DispatchController,
		statusCode: number,
		headers: Record<string, string | string[]>,
		statusMessage?: string,
	): void;
	onResponseData?(controller: DispatchController, chunk: Uint8Array): void;
	onResponseEnd?(controller: DispatchController, trailers: unknown): void;
	onResponseError?(controller: DispatchController, error: Error): void;
	/** Legacy handler shape, still accepted by undici: refused dispatches answer through it when present. */
	onError?(error: Error): void;
}

export interface Dispatcher {
	dispatch(options: DispatchOptions, handler: DispatchHandler): boolean;
}

// The platform's connection pool: Node's bundled undici Agent, taken once at
// load through the symbol undici registers it under. Nothing of the fetch
// layer (its 421 re-send, redirect following, request cloning) sits between
// the ticket and this pool.
const globalDispatcherKey = Symbol.for("undici.globalDispatcher.1");
// The platform fetch, taken at load (before the bootstrap replaces the global
// one). It is never a send path: undici creates its Agent on the first fetch,
// so a `data:` URL — no network, resolved synchronously — makes the pool exist.
const platformFetch: typeof fetch = globalThis.fetch.bind(globalThis);

function platformPool(): Dispatcher | undefined {
	const g = globalThis as unknown as Record<symbol, Dispatcher | undefined>;
	if (!g[globalDispatcherKey]) void platformFetch("data:,").catch(() => {});
	return g[globalDispatcherKey];
}

const noopController: DispatchController = {
	abort() {},
	pause() {},
	resume() {},
};

/**
 * The dispatcher of one ticket: exactly one `dispatch()` reaches the pool.
 * Any later call — a transport-layer re-send on 421 or a reset connection,
 * a redirect follow, a retry of any future layer — is refused before the
 * wire and recorded as SEND_ALREADY_ATTEMPTED on the ticket.
 */
export class TicketDispatcher implements Dispatcher {
	/** Dispatches that reached the pool: 0 or 1. */
	wireAttempts = 0;

	constructor(
		private readonly pool: Dispatcher,
		private readonly refuse: (reason: string) => void,
	) {}

	dispatch(options: DispatchOptions, handler: DispatchHandler): boolean {
		if (this.wireAttempts >= 1) {
			this.refuse(`${SEND_ALREADY_ATTEMPTED}: ${options.method} ${options.origin}${options.path}`);
			const err = new TransportRefused(
				`model proxy: the one wire send of this ticket was already attempted (${SEND_ALREADY_ATTEMPTED})`,
			);
			if (typeof handler.onResponseError === "function") handler.onResponseError(noopController, err);
			else if (typeof handler.onError === "function") handler.onError(err);
			return false;
		}
		this.wireAttempts = 1;
		return this.pool.dispatch(options, handler);
	}

	close(): Promise<void> {
		return Promise.resolve();
	}

	destroy(): Promise<void> {
		return Promise.resolve();
	}
}

/** The consumed permission of one call, usable for exactly one physical request through its fetch. */
export class SendTicket {
	used = false;
	/** Requests the guard refused after the one allowed send (retries, redirects, second sends). */
	refused: string[] = [];
	request?: CapturedRequest;
	response?: CapturedResponse;
	transportError?: string;
	/** True once the one response body was read to its end: the stream is complete, its usage is final. */
	streamComplete = false;
	/**
	 * The native usage parsed from the whole response stream as it arrives,
	 * independent of the evidence bound (a body the evidence capture
	 * truncates is still parsed to its end here).
	 */
	readonly usage = new StreamingUsage();
	/** The ticket's dispatcher: the only path of this ticket to the connection pool. */
	readonly dispatcher: TicketDispatcher;
	private readonly origin: string;
	private readonly basePath: string;
	private resolveSettled!: () => void;
	/** Resolves once the one request either failed or its response body was captured completely. */
	readonly settled: Promise<void>;
	/** The fetch implementation injected into the vendor client: this ticket's one send. */
	readonly fetch: typeof fetch = (input, init) => this.send(input, init);

	constructor(
		readonly callId: string,
		baseUrl: string,
		private readonly credential: string,
		private readonly maxEvidenceBytes: number,
		pool: Dispatcher | undefined = platformPool(),
	) {
		if (!pool) throw new Error("model proxy: the platform connection pool is not available");
		const u = new URL(baseUrl);
		this.origin = u.origin;
		this.basePath = u.pathname.replace(/\/+$/, "");
		this.settled = new Promise<void>((resolve) => {
			this.resolveSettled = resolve;
		});
		this.dispatcher = new TicketDispatcher(pool, (reason) => this.refused.push(reason));
	}

	/** Dispatches that reached the wire: 0 or 1, by construction of the dispatcher. */
	get wireAttempts(): number {
		return this.dispatcher.wireAttempts;
	}

	/** True when a second dispatch was attempted and refused. */
	get secondSendRefused(): boolean {
		return this.refused.some((r) => r.startsWith(SEND_ALREADY_ATTEMPTED));
	}

	private async send(input: string | URL | Request, init?: RequestInit): Promise<Response> {
		const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
		const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));
		const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();
		const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
		this.take(url, headers);
		let body: Uint8Array | undefined;
		try {
			body = await bodyBytesOf(init?.body ?? (input instanceof Request ? input : undefined));
		} catch (err) {
			this.refused.push(`unsupported request body (${err instanceof Error ? err.message : String(err)})`);
			this.used = false;
			throw new TransportRefused(`model proxy: call ${this.callId} carries a request body the transport cannot send`);
		}
		this.captureRequest(method, url, headers, body);
		return this.dispatchOnce(url, method, headers, body, signal ?? undefined);
	}

	/** Reserves the single send; throws when it was taken. */
	take(url: URL, headers: Headers): void {
		if (this.used) {
			this.refused.push(`second request to ${url.origin}${url.pathname}`);
			throw new TransportRefused(`model proxy: the send permission of call ${this.callId} was already used`);
		}
		if (
			url.origin !== this.origin ||
			!(url.pathname === this.basePath || url.pathname.startsWith(`${this.basePath}/`))
		) {
			this.refused.push(`request outside the route (${url.origin}${url.pathname})`);
			throw new TransportRefused(`model proxy: call ${this.callId} may only reach ${this.origin}${this.basePath}`);
		}
		const auth = headers.get("authorization");
		if (auth !== `Bearer ${this.credential}`) {
			this.refused.push("authorization header is not the route credential");
			throw new TransportRefused(`model proxy: call ${this.callId} carries a credential that is not the route's`);
		}
		for (const name of headers.keys()) {
			if (name === "cookie" || name === "proxy-authorization" || name.startsWith("x-anvilkit-")) {
				this.refused.push(`forbidden header ${name}`);
				throw new TransportRefused(`model proxy: call ${this.callId} carries a forbidden header ${name}`);
			}
		}
		this.used = true;
	}

	private captureRequest(method: string, url: URL, headers: Headers, body: Uint8Array | undefined): void {
		const text = body ? new TextDecoder().decode(body) : "";
		const truncated = text.length > this.maxEvidenceBytes;
		this.request = {
			method,
			url: `${url.origin}${url.pathname}`,
			contentType: headers.get("content-type") ?? "",
			body: truncated ? text.slice(0, this.maxEvidenceBytes) : text,
			bodyTruncated: truncated,
			bodyBytes: body?.byteLength ?? 0,
		};
	}

	private fail(err: unknown): void {
		this.transportError ??= describeError(err);
		this.resolveSettled();
	}

	/**
	 * The one dispatch: the Response the vendor client reads is built from
	 * the handler of this single call to the pool; its body and the evidence
	 * capture are fed by the same chunks. A 3xx is handed back as the
	 * response it is. The caller's signal aborts the dispatch; a canceled
	 * body stream does the same.
	 */
	private dispatchOnce(
		url: URL,
		method: string,
		headers: Headers,
		body: Uint8Array | undefined,
		signal: AbortSignal | undefined,
	): Promise<Response> {
		const headerRecord: Record<string, string> = {};
		for (const [name, value] of headers) headerRecord[name] = value;
		const captured: CapturedResponse = {
			status: 0,
			headers: {},
			body: new Uint8Array(0),
			bodyBytes: 0,
			bodyTruncated: false,
		};
		const chunks: Uint8Array[] = [];
		let kept = 0;
		let undici: DispatchController | undefined;
		let stream: ReadableStreamDefaultController<Uint8Array> | undefined;
		let started = false;
		let finished = false;
		let drainTimer: ReturnType<typeof setTimeout> | undefined;
		const finish = (err?: unknown) => {
			if (finished) return;
			finished = true;
			clearTimeout(drainTimer);
			const out = new Uint8Array(kept);
			let off = 0;
			for (const c of chunks) {
				out.set(c, off);
				off += c.byteLength;
			}
			captured.body = out;
			if (err !== undefined) this.fail(err);
			else this.resolveSettled();
		};
		const abortError = () => (signal?.reason ?? new DOMException("This operation was aborted", "AbortError")) as Error;
		const onAbort = () => {
			if (undici) undici.abort(abortError());
		};
		if (signal?.aborted) return Promise.reject(abortError());
		signal?.addEventListener("abort", onAbort, { once: true });
		return new Promise<Response>((resolve, reject) => {
			const handler: DispatchHandler = {
				onRequestStart: (controller) => {
					undici = controller;
					if (signal?.aborted) controller.abort(abortError());
				},
				onResponseStart: (controller, statusCode, rawHeaders, statusMessage) => {
					undici = controller;
					started = true;
					captured.status = statusCode;
					const responseHeaders = new Headers();
					for (const [name, value] of Object.entries(rawHeaders)) {
						const joined = Array.isArray(value) ? value.join(", ") : value;
						if (evidenceHeaders.includes(name.toLowerCase())) captured.headers[name.toLowerCase()] = joined;
						responseHeaders.set(name, joined);
					}
					this.response = captured;
					// No pause/resume between undici's controller and the stream:
					// the two flow-control mechanisms can deadlock when a pull
					// fires before a pause lands, and the route bounds already cap
					// what a send may deliver (the output bound aborts the send).
					const bodyStream = new ReadableStream<Uint8Array>({
						start: (c) => {
							stream = c;
						},
						cancel: (reason) => {
							// OpenAI 7 stops iterating at [DONE], which cancels its
							// reader before HTTP EOF. Keep capturing that same response
							// briefly; only real EOF establishes complete usage. An
							// explicit abort still stops the wire immediately, and a
							// peer that never closes remains unknown after this bound.
							if (this.usage.hasCompletionSentinel && !signal?.aborted && !finished) {
								drainTimer = setTimeout(() => controller.abort(new Error("response EOF timeout")), 1000);
								return;
							}
							controller.abort(reason instanceof Error ? reason : abortError());
						},
					});
					resolve(
						new Response(statusCode === 204 || statusCode === 304 ? null : bodyStream, {
							status: statusCode,
							statusText: statusMessage ?? "",
							headers: responseHeaders,
						}),
					);
				},
				onResponseData: (_controller, chunk) => {
					captured.bodyBytes += chunk.byteLength;
					this.usage.feed(chunk);
					if (kept < this.maxEvidenceBytes) {
						const room = this.maxEvidenceBytes - kept;
						const piece = chunk.byteLength > room ? chunk.subarray(0, room) : chunk;
						chunks.push(new Uint8Array(piece));
						kept += piece.byteLength;
						if (chunk.byteLength > room) captured.bodyTruncated = true;
					} else {
						captured.bodyTruncated = true;
					}
					if (stream) {
						try {
							stream.enqueue(new Uint8Array(chunk));
						} catch {
							// The caller closed its side; the evidence capture continues to the end.
						}
					}
				},
				onResponseEnd: () => {
					signal?.removeEventListener("abort", onAbort);
					this.streamComplete = true;
					try {
						stream?.close();
					} catch {
						// already closed or errored by the caller
					}
					finish();
				},
				onResponseError: (_controller, err) => {
					signal?.removeEventListener("abort", onAbort);
					if (started) {
						try {
							stream?.error(err);
						} catch {
							// already closed
						}
					} else {
						reject(err);
					}
					finish(err);
				},
			};
			try {
				this.dispatcher.dispatch(
					{
						origin: url.origin,
						path: `${url.pathname}${url.search}`,
						method,
						headers: headerRecord,
						body: body ?? null,
					},
					handler,
				);
			} catch (err) {
				signal?.removeEventListener("abort", onAbort);
				finish(err);
				reject(err);
			}
		});
	}
}

/** The request body as bytes: the shapes the vendor clients use; a streaming body is read to its end. */
async function bodyBytesOf(body: unknown): Promise<Uint8Array | undefined> {
	if (body === undefined || body === null) return undefined;
	if (typeof body === "string") return new TextEncoder().encode(body);
	if (body instanceof Uint8Array) return body;
	if (body instanceof ArrayBuffer) return new Uint8Array(body);
	if (ArrayBuffer.isView(body)) return new Uint8Array(body.buffer, body.byteOffset, body.byteLength);
	if (body instanceof URLSearchParams) return new TextEncoder().encode(body.toString());
	if (body instanceof Blob) return new Uint8Array(await body.arrayBuffer());
	if (body instanceof Request) return body.body ? new Uint8Array(await body.arrayBuffer()) : undefined;
	if (body instanceof ReadableStream) return new Uint8Array(await new Response(body).arrayBuffer());
	throw new Error(`body of type ${typeof body}`);
}

/** name: message, with undici's cause (its code names the socket-level fact, e.g. ECONNREFUSED). */
function describeError(err: unknown): string {
	if (!(err instanceof Error)) return String(err);
	const code = (err as { code?: string }).code;
	const cause = (err as { cause?: unknown }).cause;
	const c = cause instanceof Error ? ` (${(cause as { code?: string }).code ?? cause.name}: ${cause.message})` : "";
	return `${err.name}: ${err.message}${code && !c ? ` (${code})` : ""}${c}`;
}

/** The process-wide fetch after bootstrap: every send goes through a ticket, so a bare fetch is a defect and is refused. */
export function refusingFetch(input: string | URL | Request): Promise<Response> {
	const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
	return Promise.reject(
		new TransportRefused(
			`model proxy: no send permission for ${url.origin}${url.pathname} (fetch outside a call ticket)`,
		),
	);
}

let refused = false;

/** Replaces the global fetch once with the refusing one; tickets never use it (they dispatch on the pool directly). */
export function refuseGlobalFetch(): void {
	if (refused) return;
	refused = true;
	globalThis.fetch = refusingFetch as typeof fetch;
}

/**
 * The native usage object and response id of an OpenAI-compatible
 * chat-completions stream, parsed incrementally from the bytes as they
 * arrive: the last `usage` member any data chunk carried, as reported
 * (undefined when no chunk carried one; the domain classifies what it
 * holds). The bytes are parsed as the event stream they are; `[DONE]` and
 * non-JSON payloads carry nothing, and an event block the stream never
 * terminated with a blank line is not an event (the SSE interpretation the
 * vendor client applies as well): a truncated body reports what it
 * completed, never what it hints at.
 */
export class StreamingUsage {
	hasCompletionSentinel = false;
	private usage: unknown;
	private found = false;
	private responseId?: string;
	private readonly decoder = new TextDecoder();
	private readonly parser = createParser({
		onEvent: (event) => {
			if (this.hasCompletionSentinel) return;
			const payload = event.data.trim();
			if (payload === "[DONE]") {
				this.hasCompletionSentinel = true;
				return;
			}
			if (!payload) return;
			let chunk: unknown;
			try {
				chunk = JSON.parse(payload);
			} catch {
				return;
			}
			if (typeof chunk !== "object" || chunk === null) return;
			const c = chunk as Record<string, unknown>;
			if (typeof c.id === "string" && c.id) this.responseId ??= c.id;
			if ("usage" in c && c.usage !== null && c.usage !== undefined) {
				this.usage = c.usage;
				this.found = true;
			}
		},
	});

	feed(bytes: Uint8Array): void {
		this.parser.feed(this.decoder.decode(bytes, { stream: true }));
	}

	result(): { usage: unknown; responseId?: string } | undefined {
		if (!this.found) return undefined;
		const out: { usage: unknown; responseId?: string } = { usage: this.usage };
		if (this.responseId) out.responseId = this.responseId;
		return out;
	}
}

/** The native usage of a complete captured body (the whole stream fed at once). */
export function nativeUsageFromSse(body: Uint8Array): { usage: unknown; responseId?: string } | undefined {
	const u = new StreamingUsage();
	u.feed(body);
	return u.result();
}
