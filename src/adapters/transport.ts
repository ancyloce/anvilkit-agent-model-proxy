// The guarded transport (DD-02 §4 step 5, DD-03 §6): the only outbound HTTP
// path of the process. pi-ai 0.85.1 takes the fetch implementation of a
// call as a request option (ProviderRequestOptions.fetch, handed to the
// vendor client it builds per call), so every send is made through the
// fetch of one SendTicket — the consumed single-use permission of one call.
// The ticket allows exactly one request, the request may only reach the
// route's configured origin and path, redirects are never followed (a 3xx
// is a failed send, not a second one), the Authorization header must be
// exactly the route credential the Proxy attached (an inbound AnvilKit token
// can never be forwarded), and the request payload and the raw response are
// captured as private native evidence. The process-wide fetch is replaced
// at bootstrap by one that refuses: nothing in this process sends without a
// ticket, hence without an admission behind it.
import { createParser } from "eventsource-parser";

export interface CapturedRequest {
	method: string;
	url: string;
	contentType: string;
	body: string;
	bodyTruncated: boolean;
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

// The platform fetch, taken before the bootstrap replaces the global one.
const nativeFetch: typeof fetch = globalThis.fetch.bind(globalThis);

/** The consumed permission of one call, usable for exactly one physical request through its fetch. */
export class SendTicket {
	used = false;
	/** Requests the guard refused after the one allowed send (retries, redirects, second sends). */
	refused: string[] = [];
	request?: CapturedRequest;
	response?: CapturedResponse;
	transportError?: string;
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
	) {
		const u = new URL(baseUrl);
		this.origin = u.origin;
		this.basePath = u.pathname.replace(/\/+$/, "");
		this.settled = new Promise<void>((resolve) => {
			this.resolveSettled = resolve;
		});
	}

	private async send(input: string | URL | Request, init?: RequestInit): Promise<Response> {
		const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
		const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));
		const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();
		this.take(url, headers);
		this.captureRequest(method, url, headers, init?.body);
		let res: Response;
		try {
			res = await nativeFetch(url, { ...init, headers, redirect: "manual" });
		} catch (err) {
			this.fail(err);
			throw err;
		}
		return this.captureResponse(res);
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

	private captureRequest(method: string, url: URL, headers: Headers, body: unknown): void {
		let text = "";
		if (typeof body === "string") text = body;
		else if (body instanceof Uint8Array) text = new TextDecoder().decode(body);
		else if (body !== undefined && body !== null) text = `<${typeof body} body>`;
		const truncated = text.length > this.maxEvidenceBytes;
		this.request = {
			method,
			url: `${url.origin}${url.pathname}`,
			contentType: headers.get("content-type") ?? "",
			body: truncated ? text.slice(0, this.maxEvidenceBytes) : text,
			bodyTruncated: truncated,
		};
	}

	private fail(err: unknown): void {
		this.transportError = describeError(err);
		this.resolveSettled();
	}

	/** Captures the response head and tees the body into the evidence buffer. */
	private captureResponse(res: Response): Response {
		const headers: Record<string, string> = {};
		for (const name of evidenceHeaders) {
			const v = res.headers.get(name);
			if (v !== null) headers[name] = v;
		}
		const captured: CapturedResponse = {
			status: res.status,
			headers,
			body: new Uint8Array(0),
			bodyBytes: 0,
			bodyTruncated: false,
		};
		this.response = captured;
		if (!res.body) {
			this.resolveSettled();
			return res;
		}
		const [toCaller, toEvidence] = res.body.tee();
		const chunks: Uint8Array[] = [];
		let kept = 0;
		(async () => {
			const reader = toEvidence.getReader();
			try {
				for (;;) {
					const { done, value } = await reader.read();
					if (done) break;
					captured.bodyBytes += value.byteLength;
					if (kept < this.maxEvidenceBytes) {
						const room = this.maxEvidenceBytes - kept;
						const piece = value.byteLength > room ? value.subarray(0, room) : value;
						chunks.push(piece);
						kept += piece.byteLength;
						if (value.byteLength > room) captured.bodyTruncated = true;
					} else {
						captured.bodyTruncated = true;
					}
				}
			} catch (err) {
				this.transportError ??= describeError(err);
			} finally {
				const body = new Uint8Array(kept);
				let off = 0;
				for (const c of chunks) {
					body.set(c, off);
					off += c.byteLength;
				}
				captured.body = body;
				this.resolveSettled();
			}
		})();
		return new Response(toCaller, { status: res.status, statusText: res.statusText, headers: res.headers });
	}
}

/** name: message, with undici's cause (its code names the socket-level fact, e.g. ECONNREFUSED). */
function describeError(err: unknown): string {
	if (!(err instanceof Error)) return String(err);
	const cause = (err as { cause?: unknown }).cause;
	const c = cause instanceof Error ? ` (${(cause as { code?: string }).code ?? cause.name}: ${cause.message})` : "";
	return `${err.name}: ${err.message}${c}`;
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

/** Replaces the global fetch once with the refusing one; tickets keep the platform fetch they took at load. */
export function refuseGlobalFetch(): void {
	if (refused) return;
	refused = true;
	globalThis.fetch = refusingFetch as typeof fetch;
}

/**
 * The native usage object and response id of an OpenAI-compatible
 * chat-completions stream, from the captured SSE body: the last `usage`
 * member any data chunk carried, as reported (undefined when no chunk
 * carried one; the domain classifies what it holds). The body is parsed as
 * the event stream it is; `[DONE]` and non-JSON payloads carry nothing.
 */
export function nativeUsageFromSse(body: Uint8Array): { usage: unknown; responseId?: string } | undefined {
	let usage: unknown;
	let found = false;
	let responseId: string | undefined;
	const parser = createParser({
		onEvent: (event) => {
			const payload = event.data.trim();
			if (!payload || payload === "[DONE]") return;
			let chunk: unknown;
			try {
				chunk = JSON.parse(payload);
			} catch {
				return;
			}
			if (typeof chunk !== "object" || chunk === null) return;
			const c = chunk as Record<string, unknown>;
			if (typeof c.id === "string" && c.id) responseId ??= c.id;
			if ("usage" in c && c.usage !== null && c.usage !== undefined) {
				usage = c.usage;
				found = true;
			}
		},
	});
	// An event block the stream never terminated with a blank line is not an
	// event (the SSE interpretation the vendor client applies as well): a
	// truncated body reports what it completed, never what it hints at.
	parser.feed(new TextDecoder().decode(body));
	if (!found) return undefined;
	const out: { usage: unknown; responseId?: string } = { usage };
	if (responseId) out.responseId = responseId;
	return out;
}
