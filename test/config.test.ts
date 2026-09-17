import { describe, expect, it } from "vitest";
import { ConfigError, disabled, parseDuration, routeDisabledReasons } from "../src/config.js";
import { checkedInConfig, configFrom } from "./helpers.js";

const base = checkedInConfig();
const enabledRoute = base.replace("    enabled: false\n", "    enabled: true\n");
const devEnv = {
	ANVILKIT_MODEL_PROXY_PRINCIPALS_FILE: "/dev/null",
	ANVILKIT_MODEL_PROXY_STORE_DIR: "/tmp/x",
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
		const without = configFrom(enabledRoute.replace("mode: disabled", "mode: development"), {
			ANVILKIT_MODEL_PROXY_PRINCIPALS_FILE: "/dev/null",
			ANVILKIT_MODEL_PROXY_STORE_DIR: "/tmp/x",
		});
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
