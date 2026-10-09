import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { ConfigError, disabled, parseDuration, routeDisabledReasons } from "../src/config.js";
import { checkedInConfig, configFrom } from "./helpers.js";

const base = checkedInConfig();
const enabledRoute = base.replace("    enabled: false\n", "    enabled: true\n");
const devEnv = {
	ANVILKIT_MODEL_PROXY_PRINCIPALS_FILE: "/dev/null",
	ANVILKIT_MODEL_PROXY_STORE_DIR: "/tmp/x",
	// The Control identity files are placements the loader does not read.
	ANVILKIT_MODEL_PROXY_CONTROL_IDENTITY_CERT_FILE: "/etc/anvilkit/identity/tls.crt",
	ANVILKIT_MODEL_PROXY_CONTROL_IDENTITY_KEY_FILE: "/etc/anvilkit/identity/tls.key",
	ANVILKIT_MODEL_PROXY_CONTROL_IDENTITY_CA_FILE: "/etc/anvilkit/identity/ca.crt",
	ANVILKIT_MODEL_PROXY_CREDENTIAL_CONTROLLED_OPENAI_V1: "fixture-credential",
};

describe("configuration", () => {
	it("loads the checked-in file with the profile disabled and no served route", () => {
		const cfg = configFrom(base);
		expect(disabled(cfg)).toBe(true);
		expect(cfg.routes).toHaveLength(1);
		const route = cfg.routes[0];
		if (!route) throw new Error("route");
		expect(routeDisabledReasons(cfg, route)).toEqual([
			"enabled: false",
			"credential ANVILKIT_MODEL_PROXY_CREDENTIAL_CONTROLLED_OPENAI_V1 is not set",
		]);
		expect(cfg.credentials.size).toBe(0);
	});

	it("serves an enabled route only with its credential from the environment", () => {
		const { ANVILKIT_MODEL_PROXY_CREDENTIAL_CONTROLLED_OPENAI_V1: _credential, ...withoutCredential } = devEnv;
		const without = configFrom(enabledRoute.replace("mode: disabled", "mode: development"), withoutCredential);
		const r = without.routes[0];
		if (!r) throw new Error("route");
		expect(routeDisabledReasons(without, r)).toEqual([
			"credential ANVILKIT_MODEL_PROXY_CREDENTIAL_CONTROLLED_OPENAI_V1 is not set",
		]);
		const cfg = configFrom(enabledRoute.replace("mode: disabled", "mode: development"), devEnv);
		expect(routeDisabledReasons(cfg, cfg.routes[0] as never)).toEqual([]);
		expect(cfg.credentials.get("controlled-openai-v1")).toBe("fixture-credential");
		expect(JSON.stringify(cfg)).not.toContain("fixture-credential");
	});

	it("refuses unknown keys, secrets in the file and unallowed environment variables", () => {
		expect(() => configFrom(`${base}\nextra: 1\n`)).toThrow(/unknown key extra/);
		expect(() => configFrom(base.replace("  backend: filesystem\n", "  backend: filesystem\n  dir: /var/x\n"))).toThrow(
			/store.dir is a secret or a deployment placement/,
		);
		expect(() => configFrom(`${base}\nroutes: []\n`)).toThrow(/Map keys must be unique/);
		expect(() => configFrom(base, { ANVILKIT_MODEL_PROXY_API_KEY: "x" })).toThrow(
			/not allowed overrides: ANVILKIT_MODEL_PROXY_API_KEY/,
		);
		expect(() => configFrom(base, { ANVILKIT_MODEL_PROXY_CREDENTIAL_OTHER: "x" })).toThrow(
			/not the credential of any configured route/,
		);
		expect(() =>
			configFrom(
				base.replace(
					"credential_env: ANVILKIT_MODEL_PROXY_CREDENTIAL_CONTROLLED_OPENAI_V1",
					"credential_env: OPENAI_API_KEY",
				),
			),
		).toThrow(/credential_env must match/);
	});

	it("refuses an unimplemented SDK path, a non-object tool schema, a bad base_url and duplicate ids", () => {
		expect(() => configFrom(base.replace("api: openai-completions", "api: anthropic-messages"))).toThrow(
			/not an implemented SDK path/,
		);
		expect(() => configFrom(base.replace("          type: object\n", "          type: string\n"))).toThrow(
			/input_schema must be a JSON Schema object schema/,
		);
		expect(() =>
			configFrom(base.replace("base_url: http://127.0.0.1:1/v1", "base_url: http://user:pw@127.0.0.1:1/v1")),
		).toThrow(/without credentials/);
		expect(() => configFrom(base.replace("base_url: http://127.0.0.1:1/v1", "base_url: ftp://x/"))).toThrow(
			/absolute http\(s\) URL/,
		);
		const twice = base.replace(
			"routes:\n",
			'routes:\n  - id: controlled-openai-v1\n    api: openai-completions\n    provider: p\n    model: m\n    base_url: http://h/v1\n    credential_env: ANVILKIT_MODEL_PROXY_CREDENTIAL_X\n    limits: { max_output_tokens: 1, max_exposure: { currency: USD, amount: "1" }, max_deadline: 1s, max_frame_bytes: 1, max_output_bytes: 1, max_frames: 2, upstream_timeout: 1s, max_evidence_bytes: 1024 }\n',
		);
		expect(() => configFrom(twice)).toThrow(/declared twice/);
	});

	it("requires the development inputs once the profile is enabled and validates ranges", () => {
		expect(() => configFrom(base.replace("mode: disabled", "mode: development"))).toThrow(ConfigError);
		expect(() => configFrom(base.replace("mode: disabled", "mode: development"), devEnv)).not.toThrow();
		expect(() => configFrom(base.replace("mode: disabled", "mode: mtls"), devEnv)).toThrow(
			/identity.mtls.cert_file, key_file and ca_file are required/,
		);
		expect(() => configFrom(base.replace("timeout: 15s", "timeout: 0s"))).toThrow(/control.timeout 0ms outside/);
		expect(() => configFrom(base.replace("max_frame_bytes: 65536", "max_frame_bytes: 512"))).toThrow(
			/max_frame_bytes 512 outside/,
		);
		expect(() => configFrom(`${base}\nhealth:\n  listen: 127.0.0.1:9103\n`)).toThrow(
			/health.listen must be a listener of its own/,
		);
		expect(() => configFrom(`${base}\nhealth:\n  listen: nowhere\n`)).toThrow(
			/health.listen "nowhere" is not host:port/,
		);
		expect(configFrom(base).health.listen).toBe("127.0.0.1:9104");
		expect(configFrom(base, { ANVILKIT_MODEL_PROXY_HEALTH_LISTEN: "0.0.0.0:9104" }).health.listen).toBe("0.0.0.0:9104");
		expect(() =>
			configFrom(
				base.replace(
					'max_exposure: { currency: USD, amount: "1000000" }',
					"max_exposure: { currency: USD, amount: 0.5 }",
				),
			),
		).toThrow(/positive scale-6 decimal amount/);
		expect(() =>
			configFrom(
				base.replace("backend: filesystem", "backend: s3").replace("mode: disabled", "mode: development"),
				devEnv,
			),
		).toThrow(/STORE_S3/);
	});

	it("parses Go-style durations only", () => {
		expect(parseDuration("500ms", "k")).toBe(500);
		expect(parseDuration("2m", "k")).toBe(120_000);
		expect(() => parseDuration("1.5s", "k")).toThrow(/not a duration/);
		expect(() => parseDuration("10", "k")).toThrow(/not a duration/);
	});
});

it("loads a CSI credential file and refuses ambiguous sources without disclosing content", () => {
	const dir = mkdtempSync(join(tmpdir(), "proxy-credential-")),
		file = join(dir, "api-key");
	try {
		writeFileSync(file, "fixture-file-credential\n", { mode: 0o400 });
		const env = { ANVILKIT_MODEL_PROXY_CREDENTIAL_CONTROLLED_OPENAI_V1_FILE: file };
		expect(configFrom(base, env).credentials.get("controlled-openai-v1")).toBe("fixture-file-credential");
		expect(() => configFrom(base, { ...env, ANVILKIT_MODEL_PROXY_CREDENTIAL_CONTROLLED_OPENAI_V1: "other" })).toThrow(
			/both/,
		);
		expect(() =>
			configFrom(base, { ANVILKIT_MODEL_PROXY_CREDENTIAL_CONTROLLED_OPENAI_V1_FILE: join(dir, "missing") }),
		).toThrow(/cannot read credential file/);
	} finally {
		rmSync(dir, { recursive: true });
	}
});

describe("P0.6 secret files, TLS and SPIFFE principals", () => {
	const s3Config = base.replace("  backend: filesystem\n", "  backend: s3\n");
	const outsideDevelopment = (yaml: string) =>
		yaml.replace("development:\n  enabled: true\n", "development:\n  enabled: false\n");

	it("reads the S3 key pair from mounted files, never both sources and never echoing them", () => {
		const dir = mkdtempSync(join(tmpdir(), "proxy-s3-"));
		try {
			const id = join(dir, "access-key-id");
			const secret = join(dir, "secret-access-key");
			writeFileSync(id, "proxy-key-id\n");
			writeFileSync(secret, "proxy-secret-value\n");
			const env = {
				...devEnv,
				ANVILKIT_MODEL_PROXY_STORE_S3_ENDPOINT: "https://objects.anvilkit-data.svc:9000",
				ANVILKIT_MODEL_PROXY_STORE_S3_BUCKET: "anvilkit-model-proxy",
				ANVILKIT_MODEL_PROXY_STORE_S3_ACCESS_KEY_ID_FILE: id,
				ANVILKIT_MODEL_PROXY_STORE_S3_SECRET_ACCESS_KEY_FILE: secret,
			};
			const cfg = configFrom(s3Config.replace("mode: disabled", "mode: development"), env);
			expect(cfg.store.s3).toMatchObject({ accessKeyId: "proxy-key-id", secretAccessKey: "proxy-secret-value" });
			const empty = join(dir, "empty");
			writeFileSync(empty, "\n");
			const cases: [Record<string, string>, RegExp][] = [
				[
					{ ...env, ANVILKIT_MODEL_PROXY_STORE_S3_ACCESS_KEY_ID: "inline" },
					/store.s3.access_key_id: both direct and file sources supplied/,
				],
				[
					{ ...env, ANVILKIT_MODEL_PROXY_STORE_S3_SECRET_ACCESS_KEY: "inline" },
					/store.s3.secret_access_key: both direct and file sources supplied/,
				],
				[
					{ ...env, ANVILKIT_MODEL_PROXY_STORE_S3_SECRET_ACCESS_KEY_FILE: join(dir, "missing") },
					/store.s3.secret_access_key_file: cannot read the file/,
				],
				[
					{ ...env, ANVILKIT_MODEL_PROXY_STORE_S3_ACCESS_KEY_ID_FILE: empty },
					/store.s3.access_key_id_file: the file is empty/,
				],
			];
			for (const [e, want] of cases) {
				let message = "";
				try {
					configFrom(s3Config, e);
				} catch (err) {
					message = (err as Error).message;
				}
				expect(message).toMatch(want);
				expect(message).not.toContain("proxy-secret-value");
			}
			// The file keys are placements: refused inside the reviewed file.
			expect(() =>
				configFrom(
					base.replace("  s3:\n    region: default", `  s3:\n    access_key_id_file: ${id}\n    region: default`),
				),
			).toThrow(/comes only from the environment/);
		} finally {
			rmSync(dir, { recursive: true });
		}
	});

	it("requires an https:// store endpoint outside development; the guard admits the foundation's plaintext MinIO", () => {
		const env = { ANVILKIT_MODEL_PROXY_STORE_S3_ENDPOINT: "http://minio:9000" };
		expect(() => configFrom(outsideDevelopment(s3Config), env)).toThrow(
			/store.s3.endpoint must be https:\/\/ outside development/,
		);
		expect(configFrom(s3Config, env).store.s3.endpoint).toBe("http://minio:9000");
		expect(
			configFrom(outsideDevelopment(s3Config), { ANVILKIT_MODEL_PROXY_STORE_S3_ENDPOINT: "https://minio:9000" }).store
				.s3.endpoint,
		).toBe("https://minio:9000");
	});

	it("maps mTLS callers by SPIFFE ID and refuses common names, malformed entries and duplicates (F-P0.1-1)", () => {
		const env = {
			...devEnv,
			ANVILKIT_MODEL_PROXY_IDENTITY_CERT_FILE: "/etc/anvilkit/identity/tls.crt",
			ANVILKIT_MODEL_PROXY_IDENTITY_KEY_FILE: "/etc/anvilkit/identity/tls.key",
			ANVILKIT_MODEL_PROXY_IDENTITY_CA_FILE: "/etc/anvilkit/identity/ca.crt",
		};
		const withPrincipals = (list: string) =>
			base
				.replace("mode: disabled", "mode: mtls")
				.replace("  mtls:\n    principals: []", `  mtls:\n    principals:\n${list}`);
		const workflow =
			"      - spiffe_id: spiffe://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-workflow\n        principal_id: anvilkit-agent-workflow\n        kind: workflow\n";
		expect(configFrom(withPrincipals(workflow), env).identity.mtls.principals).toEqual([
			{
				spiffeId: "spiffe://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-workflow",
				principalId: "anvilkit-agent-workflow",
				kind: "workflow",
			},
		]);
		const cases: [string, RegExp][] = [
			["      - common_name: anvilkit-agent-workflow\n        kind: workflow\n", /unknown key common_name/],
			[`${workflow}${workflow}`, /principals\[1\].spiffe_id .* is declared twice/],
			[
				"      - spiffe_id: https://anvilkit.local/workflow\n        principal_id: w\n        kind: workflow\n",
				/principals\[0\].spiffe_id must be a SPIFFE ID/,
			],
			[
				"      - spiffe_id: spiffe://anvilkit.local\n        principal_id: w\n        kind: workflow\n",
				/principals\[0\].spiffe_id must be a SPIFFE ID/,
			],
			[
				"      - spiffe_id: spiffe://anvilkit.local/ns/a/sa/w\n        kind: workflow\n",
				/principals\[0\].principal_id must be a contract Id/,
			],
			[
				"      - spiffe_id: spiffe://anvilkit.local/ns/a/sa/w\n        principal_id: w\n        kind: admin\n",
				/principals\[0\].kind must be workflow, sidecar or control/,
			],
			[`${workflow}        extra: 1\n`, /principals\[0\]: unknown key extra/],
			["      - anvilkit-agent-workflow\n", /principals\[0\] must be a mapping/],
		];
		for (const [list, want] of cases) expect(() => configFrom(withPrincipals(list), env), list).toThrow(want);
	});
});
