// The Proxy's immutable configuration snapshot (A09 in spirit, DD-09 §4):
// defaults < the reviewed, secret-free config.yaml < the allowlisted
// ANVILKIT_MODEL_PROXY_* environment. Unknown keys, unknown environment
// variables, missing values, out-of-range values and contradictory settings
// stop the process before it listens. Provider credentials are never in the
// file: a route names the environment variable that carries its credential,
// and a route whose inputs are incomplete (no credential, no reviewed tool
// schema, an unimplemented API) is not served — nothing is invented for it.
import { readFileSync } from "node:fs";
import { parse as parseYaml } from "yaml";

export const envPrefix = "ANVILKIT_MODEL_PROXY_";
export const envConfigFile = "ANVILKIT_MODEL_PROXY_CONFIG";
export const defaultConfigFile = "config.yaml";

/** The one SDK path this build implements and inspected (delivery.md P11-02). */
export const implementedApis = ["openai-completions"] as const;
export type ImplementedApi = (typeof implementedApis)[number];

export type IdentityMode = "disabled" | "development" | "mtls";
export type PrincipalKind = "workflow" | "sidecar" | "control";

export interface Money {
	currency: string;
	amount: string;
}

export interface RouteLimits {
	maxOutputTokens: number;
	maxExposure: Money;
	maxDeadlineMs: number;
	maxFrameBytes: number;
	maxOutputBytes: number;
	maxFrames: number;
	upstreamTimeoutMs: number;
	maxEvidenceBytes: number;
}

export interface ToolSchema {
	name: string;
	description: string;
	inputSchema: Record<string, unknown>;
}

export interface Route {
	id: string;
	enabled: boolean;
	api: ImplementedApi;
	provider: string;
	model: string;
	baseUrl: string;
	credentialEnv: string;
	limits: RouteLimits;
	tools: ToolSchema[];
}

export interface MtlsFiles {
	certFile: string;
	keyFile: string;
	caFile: string;
	serverName: string;
}

export interface Config {
	http: {
		listen: string;
		maxBodyBytes: number;
		requestHeaderTimeoutMs: number;
		sseHeartbeatMs: number;
		slowConsumerGraceMs: number;
		shutdownTimeoutMs: number;
	};
	/** The plaintext probe listener: /healthz and /readyz only, no identity, no business route (the kubelet presents no client certificate). */
	health: { listen: string };
	identity: {
		mode: IdentityMode;
		owner: string;
		principalsFile: string;
		mtls: MtlsFiles & { principals: { commonName: string; kind: PrincipalKind }[] };
	};
	control: {
		address: string;
		timeoutMs: number;
		identity: { mode: "development" | "mtls"; mtls: MtlsFiles };
		admissionRetry: { initialMs: number; maxIntervalMs: number; maxAttempts: number };
	};
	store: {
		backend: "filesystem" | "s3";
		dir: string;
		s3: {
			endpoint: string;
			bucket: string;
			region: string;
			prefix: string;
			pathStyle: boolean;
			qualifyOnStart: boolean;
			accessKeyId: string;
			secretAccessKey: string;
		};
	};
	observation: {
		retryInitialMs: number;
		retryMaxIntervalMs: number;
		sweepIntervalMs: number;
		reclaimGraceMs: number;
	};
	contractsDir: string;
	routes: Route[];
	/** Credentials by route id, read from the environment; never logged or copied into the file. */
	credentials: Map<string, string>;
}

export class ConfigError extends Error {}

/** Go-style durations: 500ms, 15s, 5m, 1h (integers, one unit). */
export function parseDuration(text: unknown, key: string): number {
	if (typeof text === "number" && Number.isInteger(text) && text >= 0) {
		return text;
	}
	const m = typeof text === "string" ? /^(\d+)(ms|s|m|h)$/.exec(text) : null;
	if (!m) {
		throw new ConfigError(`${key}: ${JSON.stringify(text)} is not a duration (e.g. 500ms, 15s, 5m)`);
	}
	const n = Number(m[1]);
	const unit = m[2] as "ms" | "s" | "m" | "h";
	return n * { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[unit];
}

// Environment variables that are exact placements (key in the snapshot).
const envOverrides: Record<string, string> = {
	ANVILKIT_MODEL_PROXY_LISTEN: "http.listen",
	ANVILKIT_MODEL_PROXY_HEALTH_LISTEN: "health.listen",
	ANVILKIT_MODEL_PROXY_CONTROL_ADDRESS: "control.address",
	ANVILKIT_MODEL_PROXY_PRINCIPALS_FILE: "identity.principals_file",
	ANVILKIT_MODEL_PROXY_STORE_DIR: "store.dir",
	ANVILKIT_MODEL_PROXY_STORE_S3_ENDPOINT: "store.s3.endpoint",
	ANVILKIT_MODEL_PROXY_STORE_S3_BUCKET: "store.s3.bucket",
	ANVILKIT_MODEL_PROXY_STORE_S3_ACCESS_KEY_ID: "store.s3.access_key_id",
	ANVILKIT_MODEL_PROXY_STORE_S3_SECRET_ACCESS_KEY: "store.s3.secret_access_key",
	ANVILKIT_MODEL_PROXY_CONTRACTS_DIR: "contracts.dir",
};

// Keys that are secrets or per-deployment placements: refused inside the file.
const environmentOnly = [
	"identity.principals_file",
	"store.dir",
	"store.s3.endpoint",
	"store.s3.bucket",
	"store.s3.access_key_id",
	"store.s3.secret_access_key",
	"contracts.dir",
];

const credentialEnvPattern = /^ANVILKIT_MODEL_PROXY_CREDENTIAL_[A-Z0-9_]+$/;
const routeIdPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const toolNamePattern = /^[a-zA-Z][a-zA-Z0-9_-]{0,63}$/;
const moneyPattern = /^(0|[1-9][0-9]{0,29})$/;
const currencyPattern = /^[A-Z]{3}$/;
const listenPattern = /^[^:\s]+:\d{1,5}$/;

type Raw = Record<string, unknown>;

function isObject(v: unknown): v is Raw {
	return typeof v === "object" && v !== null && !Array.isArray(v);
}

function get(raw: Raw, path: string): unknown {
	let cur: unknown = raw;
	for (const part of path.split(".")) {
		if (!isObject(cur)) return undefined;
		cur = cur[part];
	}
	return cur;
}

function set(raw: Raw, path: string, value: unknown): void {
	const parts = path.split(".");
	let cur = raw;
	for (const part of parts.slice(0, -1)) {
		const next = cur[part];
		if (!isObject(next)) {
			const fresh: Raw = {};
			cur[part] = fresh;
			cur = fresh;
		} else {
			cur = next;
		}
	}
	cur[parts[parts.length - 1] as string] = value;
}

const defaults: Raw = {
	http: {
		listen: "127.0.0.1:9103",
		max_body_bytes: 4 * 1024 * 1024,
		request_header_timeout: "10s",
		sse_heartbeat: "15s",
		slow_consumer_grace: "10s",
		shutdown_timeout: "20s",
	},
	health: { listen: "127.0.0.1:9104" },
	identity: { mode: "disabled", owner: "anvilkit-agent-model-proxy", mtls: { principals: [] } },
	control: {
		timeout: "15s",
		identity: { mode: "development" },
		admission_retry: { initial: "500ms", max_interval: "5s", max_attempts: 5 },
	},
	store: {
		backend: "filesystem",
		s3: { region: "default", prefix: "model-proxy", path_style: true, qualify_on_start: true },
	},
	observation: { retry_initial: "1s", retry_max_interval: "30s", sweep_interval: "30s", reclaim_grace: "30s" },
	routes: [],
};

/** The keys the file may set (dotted; routes are validated separately). */
const knownKeys = new Set([
	"http.listen",
	"http.max_body_bytes",
	"http.request_header_timeout",
	"http.sse_heartbeat",
	"http.slow_consumer_grace",
	"http.shutdown_timeout",
	"health.listen",
	"identity.mode",
	"identity.owner",
	"identity.mtls.cert_file",
	"identity.mtls.key_file",
	"identity.mtls.ca_file",
	"identity.mtls.server_name",
	"identity.mtls.principals",
	"control.address",
	"control.timeout",
	"control.identity.mode",
	"control.identity.mtls.cert_file",
	"control.identity.mtls.key_file",
	"control.identity.mtls.ca_file",
	"control.identity.mtls.server_name",
	"control.admission_retry.initial",
	"control.admission_retry.max_interval",
	"control.admission_retry.max_attempts",
	"store.backend",
	"store.s3.region",
	"store.s3.prefix",
	"store.s3.path_style",
	"store.s3.qualify_on_start",
	"observation.retry_initial",
	"observation.retry_max_interval",
	"observation.sweep_interval",
	"observation.reclaim_grace",
	"routes",
]);

function leaves(raw: Raw, prefix = ""): string[] {
	const out: string[] = [];
	for (const [k, v] of Object.entries(raw)) {
		const key = prefix ? `${prefix}.${k}` : k;
		if (isObject(v) && key !== "identity.mtls.principals") {
			out.push(...leaves(v, key));
		} else {
			out.push(key);
		}
	}
	return out;
}

function deepMerge(base: Raw, over: Raw): Raw {
	const out: Raw = { ...base };
	for (const [k, v] of Object.entries(over)) {
		const cur = out[k];
		out[k] = isObject(cur) && isObject(v) ? deepMerge(cur, v) : v;
	}
	return out;
}

export function load(): Config {
	const path = process.env[envConfigFile] || defaultConfigFile;
	return loadFrom(path, process.env as Record<string, string | undefined>);
}

/** load with explicit inputs (tests). */
export function loadFrom(path: string, environ: Record<string, string | undefined>): Config {
	let text: string;
	try {
		text = readFileSync(path, "utf8");
	} catch (err) {
		throw new ConfigError(`config file ${path}: ${(err as Error).message}`);
	}
	const parsed = parseYaml(text, { uniqueKeys: true });
	if (parsed !== null && parsed !== undefined && !isObject(parsed)) {
		throw new ConfigError(`config file ${path}: the document is not a mapping`);
	}
	const file = (parsed ?? {}) as Raw;
	for (const key of leaves(file)) {
		if (environmentOnly.includes(key)) {
			throw new ConfigError(
				`config file ${path}: ${key} is a secret or a deployment placement and comes only from the environment`,
			);
		}
		if (!knownKeys.has(key) && !key.startsWith("routes.")) {
			throw new ConfigError(`config file ${path}: unknown key ${key}`);
		}
	}
	const raw = deepMerge(defaults, file);
	const credentials = new Map<string, string>();
	const credentialValues = new Map<string, string>();
	const unknown: string[] = [];
	for (const [name, value] of Object.entries(environ)) {
		if (!name.startsWith(envPrefix) || name === envConfigFile || value === undefined) continue;
		const key = envOverrides[name];
		if (key) {
			set(raw, key, value);
			continue;
		}
		if (credentialEnvPattern.test(name)) {
			credentialValues.set(name, value);
			continue;
		}
		unknown.push(name);
	}
	if (unknown.length > 0) {
		throw new ConfigError(`environment variables are not allowed overrides: ${unknown.sort().join(", ")}`);
	}
	const cfg = build(raw, credentialValues, credentials);
	return cfg;
}

function str(raw: Raw, key: string, fallback = ""): string {
	const v = get(raw, key);
	if (v === undefined || v === null) return fallback;
	if (typeof v !== "string") throw new ConfigError(`${key} must be a string`);
	return v;
}

function int(raw: Raw, key: string, lo: number, hi: number): number {
	const v = get(raw, key);
	const n = typeof v === "string" && /^\d+$/.test(v) ? Number(v) : v;
	if (typeof n !== "number" || !Number.isInteger(n) || n < lo || n > hi) {
		throw new ConfigError(`${key} ${JSON.stringify(v)} outside [${lo}, ${hi}]`);
	}
	return n;
}

function bool(raw: Raw, key: string): boolean {
	const v = get(raw, key);
	if (typeof v === "boolean") return v;
	if (v === "true" || v === "false") return v === "true";
	throw new ConfigError(`${key} must be a boolean`);
}

function duration(raw: Raw, key: string, loMs: number, hiMs: number): number {
	const ms = parseDuration(get(raw, key), key);
	if (ms < loMs || ms > hiMs) {
		throw new ConfigError(`${key} ${ms}ms outside [${loMs}ms, ${hiMs}ms]`);
	}
	return ms;
}

function build(raw: Raw, credentialValues: Map<string, string>, credentials: Map<string, string>): Config {
	const errors: string[] = [];
	const attempt = <T>(fn: () => T, fallback: T): T => {
		try {
			return fn();
		} catch (err) {
			if (err instanceof ConfigError) {
				errors.push(err.message);
				return fallback;
			}
			throw err;
		}
	};
	const listen = str(raw, "http.listen");
	if (!listenPattern.test(listen)) errors.push(`http.listen ${JSON.stringify(listen)} is not host:port`);
	const healthListen = str(raw, "health.listen");
	if (!listenPattern.test(healthListen)) errors.push(`health.listen ${JSON.stringify(healthListen)} is not host:port`);
	if (healthListen === listen && !listen.endsWith(":0")) {
		errors.push("health.listen must be a listener of its own, not http.listen (the probes carry no identity)");
	}
	const identityMode = str(raw, "identity.mode") as IdentityMode;
	if (!["disabled", "development", "mtls"].includes(identityMode)) {
		errors.push(`identity.mode ${JSON.stringify(identityMode)} is not one of disabled, development, mtls`);
	}
	const owner = str(raw, "identity.owner");
	if (!routeIdPattern.test(owner)) errors.push("identity.owner must be a contract Id");
	const principalsFile = str(raw, "identity.principals_file");
	if (identityMode === "development" && !principalsFile) {
		errors.push(
			"ANVILKIT_MODEL_PROXY_PRINCIPALS_FILE (identity.principals_file) is required for identity.mode development",
		);
	}
	const mtls: MtlsFiles = {
		certFile: str(raw, "identity.mtls.cert_file"),
		keyFile: str(raw, "identity.mtls.key_file"),
		caFile: str(raw, "identity.mtls.ca_file"),
		serverName: str(raw, "identity.mtls.server_name"),
	};
	const principalsRaw = get(raw, "identity.mtls.principals");
	const principals: { commonName: string; kind: PrincipalKind }[] = [];
	if (!Array.isArray(principalsRaw)) {
		errors.push("identity.mtls.principals must be a list");
	} else {
		principalsRaw.forEach((p, i) => {
			if (
				!isObject(p) ||
				typeof p.common_name !== "string" ||
				!p.common_name ||
				!["workflow", "sidecar", "control"].includes(String(p.kind))
			) {
				errors.push(`identity.mtls.principals[${i}] needs common_name and kind (workflow, sidecar or control)`);
				return;
			}
			for (const k of Object.keys(p)) {
				if (k !== "common_name" && k !== "kind") errors.push(`identity.mtls.principals[${i}]: unknown key ${k}`);
			}
			principals.push({ commonName: p.common_name, kind: p.kind as PrincipalKind });
		});
	}
	if (identityMode === "mtls") {
		if (!mtls.certFile || !mtls.keyFile || !mtls.caFile) {
			errors.push("identity.mtls.cert_file, key_file and ca_file are required for identity.mode mtls");
		}
		if (principals.length === 0)
			errors.push("identity.mtls.principals must name at least one caller for identity.mode mtls");
	}
	const controlAddress = str(raw, "control.address");
	if (identityMode !== "disabled" && !controlAddress)
		errors.push("control.address is required unless identity.mode is disabled");
	const controlIdentity = str(raw, "control.identity.mode");
	if (!["development", "mtls"].includes(controlIdentity)) {
		errors.push(`control.identity.mode ${JSON.stringify(controlIdentity)} is not one of development, mtls`);
	}
	const controlMtls: MtlsFiles = {
		certFile: str(raw, "control.identity.mtls.cert_file"),
		keyFile: str(raw, "control.identity.mtls.key_file"),
		caFile: str(raw, "control.identity.mtls.ca_file"),
		serverName: str(raw, "control.identity.mtls.server_name"),
	};
	if (controlIdentity === "mtls" && (!controlMtls.certFile || !controlMtls.keyFile || !controlMtls.caFile)) {
		errors.push("control.identity.mtls.cert_file, key_file and ca_file are required for control.identity.mode mtls");
	}
	const backend = str(raw, "store.backend");
	if (!["filesystem", "s3"].includes(backend))
		errors.push(`store.backend ${JSON.stringify(backend)} is not one of filesystem, s3`);
	const storeDir = str(raw, "store.dir");
	const s3 = {
		endpoint: str(raw, "store.s3.endpoint"),
		bucket: str(raw, "store.s3.bucket"),
		region: str(raw, "store.s3.region"),
		prefix: str(raw, "store.s3.prefix"),
		pathStyle: attempt(() => bool(raw, "store.s3.path_style"), true),
		qualifyOnStart: attempt(() => bool(raw, "store.s3.qualify_on_start"), true),
		accessKeyId: str(raw, "store.s3.access_key_id"),
		secretAccessKey: str(raw, "store.s3.secret_access_key"),
	};
	if (identityMode !== "disabled") {
		if (backend === "filesystem" && !storeDir)
			errors.push("ANVILKIT_MODEL_PROXY_STORE_DIR (store.dir) is required for store.backend filesystem");
		if (backend === "s3" && (!s3.endpoint || !s3.bucket || !s3.accessKeyId || !s3.secretAccessKey)) {
			errors.push(
				"ANVILKIT_MODEL_PROXY_STORE_S3_{ENDPOINT,BUCKET,ACCESS_KEY_ID,SECRET_ACCESS_KEY} are required for store.backend s3",
			);
		}
	}
	const routesRaw = get(raw, "routes");
	const routes: Route[] = [];
	if (!Array.isArray(routesRaw)) {
		errors.push("routes must be a list");
	} else {
		const ids = new Set<string>();
		routesRaw.forEach((r, i) => {
			const prefix = `routes[${i}]`;
			if (!isObject(r)) {
				errors.push(`${prefix} must be a mapping`);
				return;
			}
			const known = ["id", "enabled", "api", "provider", "model", "base_url", "credential_env", "limits", "tools"];
			for (const k of Object.keys(r)) {
				if (!known.includes(k)) errors.push(`${prefix}: unknown key ${k}`);
			}
			const id = String(r.id ?? "");
			if (!routeIdPattern.test(id)) errors.push(`${prefix}.id must be a contract Id`);
			if (ids.has(id)) errors.push(`${prefix}.id ${id} is declared twice`);
			ids.add(id);
			const api = String(r.api ?? "");
			if (!(implementedApis as readonly string[]).includes(api)) {
				errors.push(
					`${prefix}.api ${JSON.stringify(api)} is not an implemented SDK path (${implementedApis.join(", ")})`,
				);
			}
			const provider = String(r.provider ?? "");
			const model = String(r.model ?? "");
			if (!provider || provider.length > 64) errors.push(`${prefix}.provider is required (at most 64 characters)`);
			if (!model || model.length > 128) errors.push(`${prefix}.model is required (at most 128 characters)`);
			const baseUrl = String(r.base_url ?? "");
			let origin = "";
			try {
				const u = new URL(baseUrl);
				if (u.protocol !== "http:" && u.protocol !== "https:") throw new Error("scheme");
				if (u.username || u.password || u.hash) throw new Error("credentials or fragment");
				origin = u.origin;
			} catch {
				errors.push(`${prefix}.base_url must be an absolute http(s) URL without credentials`);
			}
			const credentialEnv = String(r.credential_env ?? "");
			if (!credentialEnvPattern.test(credentialEnv)) {
				errors.push(`${prefix}.credential_env must match ANVILKIT_MODEL_PROXY_CREDENTIAL_<NAME>`);
			}
			const enabled = r.enabled === true;
			const lim = isObject(r.limits) ? r.limits : {};
			const knownLimits = [
				"max_output_tokens",
				"max_exposure",
				"max_deadline",
				"max_frame_bytes",
				"max_output_bytes",
				"max_frames",
				"upstream_timeout",
				"max_evidence_bytes",
			];
			for (const k of Object.keys(lim)) {
				if (!knownLimits.includes(k)) errors.push(`${prefix}.limits: unknown key ${k}`);
			}
			const lraw: Raw = { l: lim };
			const exposure = isObject(lim.max_exposure) ? lim.max_exposure : {};
			const maxExposure: Money = { currency: String(exposure.currency ?? ""), amount: String(exposure.amount ?? "") };
			if (
				!currencyPattern.test(maxExposure.currency) ||
				!moneyPattern.test(maxExposure.amount) ||
				maxExposure.amount === "0"
			) {
				errors.push(`${prefix}.limits.max_exposure needs a currency and a positive scale-6 decimal amount`);
			}
			const limits: RouteLimits = {
				maxOutputTokens: attempt(() => int(lraw, "l.max_output_tokens", 1, 262144), 1),
				maxExposure,
				maxDeadlineMs: attempt(() => duration(lraw, "l.max_deadline", 1000, 24 * 3_600_000), 1000),
				maxFrameBytes: attempt(() => int(lraw, "l.max_frame_bytes", 1024, 4 * 1024 * 1024), 1024),
				maxOutputBytes: attempt(() => int(lraw, "l.max_output_bytes", 1, 64 * 1024 * 1024), 1),
				maxFrames: attempt(() => int(lraw, "l.max_frames", 2, 1_000_000), 2),
				upstreamTimeoutMs: attempt(() => duration(lraw, "l.upstream_timeout", 1000, 3_600_000), 1000),
				maxEvidenceBytes: attempt(() => int(lraw, "l.max_evidence_bytes", 1024, 256 * 1024 * 1024), 1024),
			};
			const tools: ToolSchema[] = [];
			const toolsRaw = r.tools ?? [];
			if (!Array.isArray(toolsRaw)) {
				errors.push(`${prefix}.tools must be a list`);
			} else {
				const names = new Set<string>();
				toolsRaw.forEach((t, j) => {
					const tp = `${prefix}.tools[${j}]`;
					if (!isObject(t)) {
						errors.push(`${tp} must be a mapping`);
						return;
					}
					for (const k of Object.keys(t)) {
						if (!["name", "description", "input_schema"].includes(k)) errors.push(`${tp}: unknown key ${k}`);
					}
					const name = String(t.name ?? "");
					if (!toolNamePattern.test(name)) errors.push(`${tp}.name must match the contract tool name pattern`);
					if (names.has(name)) errors.push(`${tp}.name ${name} is declared twice`);
					names.add(name);
					const description = String(t.description ?? "");
					if (description.length > 4096) errors.push(`${tp}.description exceeds 4096 characters`);
					if (!isObject(t.input_schema) || t.input_schema.type !== "object") {
						errors.push(`${tp}.input_schema must be a JSON Schema object schema (type: object)`);
						return;
					}
					tools.push({ name, description, inputSchema: t.input_schema });
				});
			}
			if (errors.length === 0 || origin) {
				routes.push({
					id,
					enabled,
					api: api as ImplementedApi,
					provider,
					model,
					baseUrl,
					credentialEnv,
					limits,
					tools,
				});
			}
		});
	}
	// Route credentials: read once, by the names the routes declare. A
	// credential variable no route declares is an unallowed override.
	const declared = new Set(routes.map((r) => r.credentialEnv));
	for (const name of credentialValues.keys()) {
		if (!declared.has(name) && !(name.endsWith("_FILE") && declared.has(name.slice(0, -5))))
			errors.push(`environment variable ${name} is not the credential of any configured route`);
	}
	for (const r of routes) {
		let v = credentialValues.get(r.credentialEnv);
		const file = credentialValues.get(`${r.credentialEnv}_FILE`);
		if (v !== undefined && file !== undefined)
			errors.push(`${r.credentialEnv}: both direct and file credential sources supplied`);
		else if (file !== undefined) {
			try {
				v = readFileSync(file, "utf8").trim();
			} catch {
				errors.push(`${r.credentialEnv}: cannot read credential file`);
			}
			if (!v) errors.push(`${r.credentialEnv}: credential file is empty`);
		}
		if (v) credentials.set(r.id, v);
	}
	const cfg: Config = {
		http: {
			listen,
			maxBodyBytes: attempt(() => int(raw, "http.max_body_bytes", 1024, 1024 * 1024 * 1024), 1024),
			requestHeaderTimeoutMs: attempt(() => duration(raw, "http.request_header_timeout", 1000, 600_000), 1000),
			sseHeartbeatMs: attempt(() => duration(raw, "http.sse_heartbeat", 1000, 600_000), 1000),
			slowConsumerGraceMs: attempt(() => duration(raw, "http.slow_consumer_grace", 1000, 600_000), 1000),
			shutdownTimeoutMs: attempt(() => duration(raw, "http.shutdown_timeout", 1000, 300_000), 1000),
		},
		health: { listen: healthListen },
		identity: { mode: identityMode, owner, principalsFile, mtls: { ...mtls, principals } },
		control: {
			address: controlAddress,
			timeoutMs: attempt(() => duration(raw, "control.timeout", 1000, 300_000), 1000),
			identity: { mode: controlIdentity as "development" | "mtls", mtls: controlMtls },
			admissionRetry: {
				initialMs: attempt(() => duration(raw, "control.admission_retry.initial", 10, 60_000), 10),
				maxIntervalMs: attempt(() => duration(raw, "control.admission_retry.max_interval", 10, 300_000), 10),
				maxAttempts: attempt(() => int(raw, "control.admission_retry.max_attempts", 1, 20), 1),
			},
		},
		store: { backend: backend as "filesystem" | "s3", dir: storeDir, s3 },
		observation: {
			retryInitialMs: attempt(() => duration(raw, "observation.retry_initial", 10, 60_000), 10),
			retryMaxIntervalMs: attempt(() => duration(raw, "observation.retry_max_interval", 10, 600_000), 10),
			sweepIntervalMs: attempt(() => duration(raw, "observation.sweep_interval", 1000, 3_600_000), 1000),
			reclaimGraceMs: attempt(() => duration(raw, "observation.reclaim_grace", 0, 3_600_000), 0),
		},
		contractsDir: str(raw, "contracts.dir"),
		routes,
		credentials,
	};
	if (cfg.control.admissionRetry.maxIntervalMs < cfg.control.admissionRetry.initialMs) {
		errors.push("control.admission_retry.max_interval must be at least control.admission_retry.initial");
	}
	if (cfg.observation.retryMaxIntervalMs < cfg.observation.retryInitialMs) {
		errors.push("observation.retry_max_interval must be at least observation.retry_initial");
	}
	if (errors.length > 0) {
		throw new ConfigError(errors.join("\n"));
	}
	return cfg;
}

/** Disabled reports whether the identity inputs keep the profile disabled: the process then exits instead of serving. */
export function disabled(cfg: Config): boolean {
	return cfg.identity.mode === "disabled";
}

/**
 * The reasons a route is not served, empty for a served route: the file may
 * declare a route whose deployment inputs are incomplete; such a route is
 * denied, never approximated.
 */
export function routeDisabledReasons(cfg: Config, route: Route): string[] {
	const reasons: string[] = [];
	if (!route.enabled) reasons.push("enabled: false");
	if (!cfg.credentials.has(route.id)) reasons.push(`credential ${route.credentialEnv} is not set`);
	return reasons;
}
