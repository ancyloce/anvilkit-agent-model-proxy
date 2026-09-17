// An in-process Proxy for the scenarios: the real call service, HTTP
// transport, ticket-bound transport and filesystem store over a scratch
// directory, against the fake Control and the countable upstream. The
// client side speaks plain node:http and parses the event stream with
// eventsource-parser.
import { writeFileSync } from "node:fs";
import { type Agent, request as httpRequest, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import path from "node:path";
import { createParser } from "eventsource-parser";
import { ControlClient } from "../src/adapters/control.js";
import { CallStore, FilesystemStore, type ObjectStore } from "../src/adapters/store.js";
import { CallService } from "../src/application/calls.js";
import type { Config } from "../src/config.js";
import { Contract, type ModelCallRequest, type StreamFrame } from "../src/contracts.js";
import { type Logger, silentLogger } from "../src/log.js";
import { createHealthServer, createServer, DevelopmentIdentity } from "../src/transport/http.js";
import { checkedInConfig, configFrom, scratch } from "./helpers.js";

export const token = "workflow-token-fixture";
export const sidecarToken = "sidecar-token-fixture";
export const controlToken = "control-token-fixture";
export const credential = "fixture-credential";
export const digest = "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3";

export interface ProxyOptions {
	upstreamUrl: string;
	controlAddress: string;
	storeDir?: string;
	instanceId?: string;
	log?: Logger;
	configEdits?: (yaml: string) => string;
	pollMs?: number;
	now?: () => number;
	/** Wraps the object store (fault injection in the scenarios). */
	objects?: (inner: ObjectStore) => ObjectStore;
}

export interface RunningProxy {
	url: string;
	/** The business listener (its connections are observable in the scenarios). */
	server: Server;
	/** The probe listener (plaintext, /healthz and /readyz only). */
	healthUrl: string;
	calls: CallService;
	store: CallStore;
	storeDir: string;
	cfg: Config;
	close(): Promise<void>;
}

const contract = new Contract();

export function proxyConfig(o: ProxyOptions): Config {
	const dir = o.storeDir ?? scratch();
	const principals = path.join(scratch(), "principals.json");
	writeFileSync(
		principals,
		JSON.stringify({
			[token]: { principalId: "anvilkit-agent-workflow", kind: "workflow" },
			[sidecarToken]: { principalId: "anvilkit-job-access-sidecar", kind: "sidecar" },
			[controlToken]: { principalId: "anvilkit-agent-control", kind: "control" },
		}),
	);
	let yaml = checkedInConfig()
		.replace("mode: disabled", "mode: development")
		.replace("    enabled: false\n", "    enabled: true\n")
		.replace("base_url: http://127.0.0.1:1/v1", `base_url: ${o.upstreamUrl}/v1`)
		.replace(
			"  retry_initial: 1s\n  retry_max_interval: 30s\n  sweep_interval: 30s\n  reclaim_grace: 30s",
			"  retry_initial: 50ms\n  retry_max_interval: 200ms\n  sweep_interval: 1s\n  reclaim_grace: 500ms",
		)
		.replace(
			"    initial: 500ms\n    max_interval: 5s\n    max_attempts: 5",
			"    initial: 50ms\n    max_interval: 300ms\n    max_attempts: 3",
		)
		.replace("  sse_heartbeat: 15s\n  slow_consumer_grace: 10s", "  sse_heartbeat: 1s\n  slow_consumer_grace: 1s")
		.replace("listen: 127.0.0.1:9103", "listen: 127.0.0.1:0");
	// The health listener binds an ephemeral port of its own.
	yaml = `${yaml}\nhealth:\n  listen: 127.0.0.1:0\n`;
	if (o.configEdits) yaml = o.configEdits(yaml);
	return configFrom(yaml, {
		ANVILKIT_MODEL_PROXY_CONTROL_ADDRESS: o.controlAddress,
		ANVILKIT_MODEL_PROXY_PRINCIPALS_FILE: principals,
		ANVILKIT_MODEL_PROXY_STORE_DIR: dir,
		ANVILKIT_MODEL_PROXY_CREDENTIAL_CONTROLLED_OPENAI_V1: credential,
	});
}

export async function startProxy(o: ProxyOptions): Promise<RunningProxy> {
	const cfg = proxyConfig(o);
	const filesystem = new FilesystemStore(cfg.store.dir);
	await filesystem.qualify();
	const store = new CallStore(o.objects ? o.objects(filesystem) : filesystem);
	const control = new ControlClient(cfg.control.address, cfg.control.timeoutMs, cfg.control.identity);
	const calls = new CallService({
		cfg,
		contract,
		control,
		store,
		log: o.log ?? silentLogger,
		instanceId: o.instanceId ?? "test",
		pollMs: o.pollMs ?? 50,
		now: o.now,
	});
	const server = createServer({
		cfg,
		contract,
		calls,
		identity: new DevelopmentIdentity(cfg.identity.principalsFile),
		log: o.log ?? silentLogger,
	});
	await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
	const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
	const health = createHealthServer(() => true);
	await new Promise<void>((r) => health.listen(0, "127.0.0.1", r));
	const healthUrl = `http://127.0.0.1:${(health.address() as AddressInfo).port}`;
	return {
		url,
		server,
		healthUrl,
		calls,
		store,
		storeDir: cfg.store.dir,
		cfg,
		close: async () => {
			await calls.close();
			server.closeAllConnections();
			await new Promise<void>((r) => server.close(() => r()));
			health.closeAllConnections();
			await new Promise<void>((r) => health.close(() => r()));
			control.close();
		},
	};
}

export interface Reply {
	status: number;
	headers: Record<string, string>;
	text: string;
	frames: StreamFrame[];
	json?: unknown;
}

/** One HTTP request; an SSE answer is collected into frames (optionally stopping early); `agent` selects the connection reuse. */
export function call(
	url: string,
	method: string,
	pathname: string,
	body?: string,
	auth: string | null = token,
	stopAfter?: (frames: StreamFrame[]) => boolean,
	agent?: Agent,
): Promise<Reply> {
	return new Promise((resolve, reject) => {
		const u = new URL(pathname, url);
		const headers: Record<string, string> = { "content-type": "application/json" };
		if (auth !== null) headers.authorization = `Bearer ${auth}`;
		const req = httpRequest({ method, hostname: u.hostname, port: u.port, path: u.pathname, headers, agent }, (res) => {
			const chunks: Buffer[] = [];
			const frames: StreamFrame[] = [];
			const decoder = new TextDecoder();
			const parser = createParser({ onEvent: (e) => frames.push(JSON.parse(e.data) as StreamFrame) });
			let stopped = false;
			res.on("data", (c: Buffer) => {
				chunks.push(c);
				if (res.headers["content-type"]?.startsWith("text/event-stream")) {
					parser.feed(decoder.decode(c, { stream: true }));
					if (!stopped && stopAfter?.(frames)) {
						stopped = true;
						res.destroy();
						resolve({ status: res.statusCode ?? 0, headers: flat(res.headers), text: "", frames });
					}
				}
			});
			res.on("end", () => {
				if (stopped) return;
				const text = Buffer.concat(chunks).toString("utf8");
				const reply: Reply = { status: res.statusCode ?? 0, headers: flat(res.headers), text, frames };
				if (res.headers["content-type"]?.startsWith("application/json")) reply.json = JSON.parse(text);
				resolve(reply);
			});
			res.on("error", (e) => (stopped ? undefined : reject(e)));
		});
		req.on("error", reject);
		if (body !== undefined) req.write(body);
		req.end();
	});
}

function flat(h: Record<string, string | string[] | undefined>): Record<string, string> {
	const out: Record<string, string> = {};
	for (const [k, v] of Object.entries(h)) out[k] = Array.isArray(v) ? v.join(",") : (v ?? "");
	return out;
}

export function requestBody(callId: string, overrides: Partial<ModelCallRequest> = {}): string {
	const req: ModelCallRequest = {
		callId,
		binding: {
			tenantId: "tenant_a",
			operationId: "op_1",
			attemptId: "att_1",
			instanceId: "inst_1",
			executionEpoch: "1",
		},
		routeId: "controlled-openai-v1",
		requestDigest: digest,
		messages: [
			{ role: "system", content: "You are the planner." },
			{ role: "user", content: "Plan the hero component." },
		],
		maxOutputTokens: 256,
		maxExposure: { currency: "USD", amount: "1000" },
		deadline: new Date(Date.now() + 60_000).toISOString(),
		...overrides,
	};
	return JSON.stringify(req);
}
