// The HTTP transport of openapi/model-proxy.yaml: the caller's identity
// (DEVELOPMENT_ONLY bearer principals, or the workload client certificate),
// strict bodies, the three routes and a bounded SSE writer. Inbound tokens
// authenticate the caller and go nowhere else; every route receives the
// authenticated principal, so a query, a cancel or a reentry is answered
// for the call's own caller only (the application decides). The stream is
// encoded by eventsource-encoder (frames as `data`, the sequence as `id`,
// keepalive comments), drops a slow consumer without touching the send,
// and never carries native bodies. The stream's lifecycle is the
// response's (its close event, a drain awaited through events.once under an
// abort signal), so a keep-alive connection serving many calls accumulates
// no listener or timer. The probes (/healthz, /readyz) live on a plaintext
// listener of their own: the kubelet presents no client certificate, so
// they never share the mTLS business listener.
import { randomBytes, timingSafeEqual } from "node:crypto";
import { once } from "node:events";
import { readFileSync } from "node:fs";
import { createServer as createHttpServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import type { TLSSocket } from "node:tls";
import { encode, encodeComment } from "eventsource-encoder";
import { CallError, type CallService, type Principal, statusOf } from "../application/calls.js";
import type { Config } from "../config.js";
import type { Contract, ErrorCode, ErrorEnvelope, ModelCallRequest, StreamFrame } from "../contracts.js";
import { ContractViolation } from "../contracts.js";
import type { Logger } from "../log.js";

export interface Identity {
	authenticate(req: IncomingMessage): Principal | undefined;
	describe(): string;
}

/** DEVELOPMENT_ONLY: bearer tokens of a principals file ({"<token>": {"principalId": "...", "kind": "workflow"|"sidecar"|"control"}}). */
export class DevelopmentIdentity implements Identity {
	private readonly byToken = new Map<string, Principal>();

	constructor(principalsFile: string) {
		const raw = JSON.parse(readFileSync(principalsFile, "utf8")) as Record<
			string,
			{ principalId?: string; kind?: string }
		>;
		for (const [token, p] of Object.entries(raw)) {
			if (
				!token ||
				!p ||
				typeof p.principalId !== "string" ||
				!p.principalId ||
				(p.kind !== "workflow" && p.kind !== "sidecar" && p.kind !== "control")
			) {
				throw new Error(
					"principals file: every entry needs a token, a principalId and a kind (workflow, sidecar or control)",
				);
			}
			this.byToken.set(token, { id: p.principalId, kind: p.kind });
		}
		if (this.byToken.size === 0) throw new Error("principals file names no principal");
	}

	describe(): string {
		return `development bearer principals (${this.byToken.size}, DEVELOPMENT_ONLY)`;
	}

	authenticate(req: IncomingMessage): Principal | undefined {
		const auth = req.headers.authorization ?? "";
		if (!auth.startsWith("Bearer ")) return undefined;
		const token = Buffer.from(auth.slice(7));
		for (const [known, principal] of this.byToken) {
			const k = Buffer.from(known);
			if (k.length === token.length && timingSafeEqual(k, token)) return principal;
		}
		return undefined;
	}
}

/** Workload mTLS: the client certificate's common name selects the principal. */
export class MtlsIdentity implements Identity {
	private readonly byName = new Map<string, Principal>();

	constructor(principals: { commonName: string; kind: Principal["kind"] }[]) {
		for (const p of principals) this.byName.set(p.commonName, { id: p.commonName, kind: p.kind });
	}

	describe(): string {
		return `mtls client certificates (${this.byName.size} principals)`;
	}

	authenticate(req: IncomingMessage): Principal | undefined {
		const socket = req.socket as TLSSocket;
		if (typeof socket.getPeerCertificate !== "function" || !socket.authorized) return undefined;
		const cert = socket.getPeerCertificate();
		const cn = cert?.subject?.CN;
		return typeof cn === "string" && cn ? this.byName.get(cn) : undefined;
	}
}

export interface HttpDeps {
	cfg: Config;
	contract: Contract;
	calls: CallService;
	identity: Identity;
	log: Logger;
}

function requestId(): string {
	return `req_${randomBytes(8).toString("hex")}`;
}

function writeError(res: ServerResponse, id: string, code: ErrorCode, message: string, retryable: boolean): void {
	const body: ErrorEnvelope = { error: { code, message: message.slice(0, 512), requestId: id, retryable } };
	res.writeHead(statusOf(code), { "content-type": "application/json", "cache-control": "no-store" });
	res.end(JSON.stringify(body));
}

async function readBody(req: IncomingMessage, limit: number): Promise<string> {
	const chunks: Buffer[] = [];
	let size = 0;
	for await (const chunk of req) {
		size += (chunk as Buffer).length;
		if (size > limit) throw new CallError("INVALID_ARGUMENT", `request body exceeds ${limit} bytes`);
		chunks.push(chunk as Buffer);
	}
	return Buffer.concat(chunks).toString("utf8");
}

const callPath = /^\/api\/v1\/model-calls\/([A-Za-z0-9][A-Za-z0-9._:-]{0,127})(\/cancellations)?$/;

/** The business listener: plaintext under the development identity, TLS with mandatory client certificates under mtls. */
export function createServer(d: HttpDeps): Server {
	const handler = (req: IncomingMessage, res: ServerResponse) => {
		void handle(d, req, res);
	};
	if (d.cfg.identity.mode === "mtls") {
		const m = d.cfg.identity.mtls;
		return createHttpsServer(
			{
				cert: readFileSync(m.certFile),
				key: readFileSync(m.keyFile),
				ca: readFileSync(m.caFile),
				requestCert: true,
				rejectUnauthorized: true,
				minVersion: "TLSv1.3",
			},
			handler,
		);
	}
	return createHttpServer(handler);
}

/**
 * The probe listener: GET /healthz answers while the process runs, GET
 * /readyz while the service accepts calls; nothing else is served and no
 * identity is checked, so it must not be exposed beyond the Pod's probes.
 */
export function createHealthServer(ready: () => boolean): Server {
	return createHttpServer((req, res) => {
		const url = new URL(req.url ?? "/", "http://proxy");
		if (req.method === "GET" && url.pathname === "/healthz") {
			res.writeHead(200, { "content-type": "text/plain", "cache-control": "no-store" }).end("ok");
			return;
		}
		if (req.method === "GET" && url.pathname === "/readyz") {
			const ok = ready();
			res
				.writeHead(ok ? 200 : 503, { "content-type": "text/plain", "cache-control": "no-store" })
				.end(ok ? "ready" : "not ready");
			return;
		}
		res.writeHead(404, { "content-type": "text/plain" }).end("not found");
	});
}

async function handle(d: HttpDeps, req: IncomingMessage, res: ServerResponse): Promise<void> {
	const id = requestId();
	const url = new URL(req.url ?? "/", "http://proxy");
	try {
		const principal = d.identity.authenticate(req);
		if (!principal) {
			writeError(res, id, "UNAUTHENTICATED", "no accepted caller identity", false);
			return;
		}
		if (req.method === "POST" && url.pathname === "/api/v1/model-calls") {
			const text = await readBody(req, d.cfg.http.maxBodyBytes);
			const request = d.contract.parse<ModelCallRequest>("ModelCallRequest", text);
			const stream = await d.calls.open(principal, request);
			await writeSse(d, res, request.callId, stream);
			return;
		}
		const m = callPath.exec(url.pathname);
		if (m?.[1] && req.method === "GET" && !m[2]) {
			const call = await d.calls.get(principal, m[1]);
			res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" }).end(JSON.stringify(call));
			return;
		}
		if (m?.[1] && req.method === "POST" && m[2]) {
			await readBody(req, 4096);
			const call = await d.calls.cancel(principal, m[1]);
			res.writeHead(202, { "content-type": "application/json", "cache-control": "no-store" }).end(JSON.stringify(call));
			return;
		}
		writeError(res, id, "NOT_FOUND", "no such route", false);
	} catch (err) {
		if (res.headersSent) {
			d.log.warn("request failed after the response started", {
				requestId: id,
				error: err instanceof Error ? err.message : String(err),
			});
			res.destroy();
			return;
		}
		if (err instanceof ContractViolation) {
			writeError(res, id, "INVALID_ARGUMENT", err.message, false);
			return;
		}
		if (err instanceof CallError) {
			writeError(res, id, err.code, err.message, err.retryable);
			return;
		}
		d.log.error("request failed", {
			requestId: id,
			path: url.pathname,
			error: err instanceof Error ? err.message : String(err),
		});
		writeError(res, id, "DEPENDENCY_UNAVAILABLE", "internal failure", true);
	}
}

/**
 * Writes the frames as SSE (id = sequence); a slow consumer is dropped after
 * the grace, the send continues. Everything registered here belongs to this
 * response: the close listener (the connection went away, or the response
 * completed) is removed when the write ends, a drain is awaited through
 * events.once under a signal that fires on the grace or on the close, and
 * the heartbeat timer is cleared — a keep-alive connection serving one call
 * after another keeps nothing of the earlier ones.
 */
async function writeSse(
	d: HttpDeps,
	res: ServerResponse,
	callId: string,
	frames: AsyncGenerator<StreamFrame>,
): Promise<void> {
	res.writeHead(200, {
		"content-type": "text/event-stream",
		"cache-control": "no-store",
		connection: "keep-alive",
		"x-accel-buffering": "no",
	});
	res.flushHeaders();
	const closing = new AbortController();
	const onClose = () => closing.abort();
	res.once("close", onClose);
	const heartbeat = setInterval(() => {
		if (!closing.signal.aborted) res.write(encodeComment("keepalive"));
	}, d.cfg.http.sseHeartbeatMs);
	const write = async (chunk: string): Promise<boolean> => {
		if (closing.signal.aborted) return false;
		if (res.write(chunk)) return true;
		try {
			await once(res, "drain", {
				signal: AbortSignal.any([closing.signal, AbortSignal.timeout(d.cfg.http.slowConsumerGraceMs)]),
			});
			return true;
		} catch {
			if (!closing.signal.aborted) {
				d.log.warn("slow consumer dropped; the send continues", { callId });
				closing.abort();
				res.destroy();
			}
			return false;
		}
	};
	try {
		for await (const f of frames) {
			const violations = d.contract.check("StreamFrame", f);
			if (violations.length > 0) {
				// A defect of this service, never hidden by skipping the frame: the
				// caller sees an interrupted stream, not a gap or a missing final frame.
				d.log.error("frame outside the contract; the stream is cut", { callId, sequence: f.sequence, type: f.type });
				closing.abort();
				res.destroy();
				break;
			}
			if (!(await write(encode({ id: f.sequence, data: JSON.stringify(f) })))) break;
		}
	} finally {
		clearInterval(heartbeat);
		res.off("close", onClose);
		await frames.return(undefined).catch(() => undefined);
		if (!closing.signal.aborted) res.end();
	}
}
