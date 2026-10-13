// P2.3: the unpaid route health probe and its effect on new calls.
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { probeModels, RouteHealth } from "../src/adapters/probe.js";
import type { Config, Route } from "../src/config.js";
import { silentLogger } from "../src/log.js";

let server: Server | undefined;
afterEach(() => server?.close());

function listen(answer: (path: string, auth: string | undefined) => number): Promise<string> {
	const listener = createServer((req, res) => {
		res.statusCode = answer(req.url ?? "", req.headers.authorization);
		res.end("{}");
	});
	server = listener;
	return new Promise((resolve) =>
		listener.listen(0, "127.0.0.1", () => resolve(`http://127.0.0.1:${(listener.address() as AddressInfo).port}/v1`)),
	);
}

const route = (baseUrl: string): Route =>
	({
		id: "r1",
		enabled: true,
		baseUrl,
		credentialEnv: "ANVILKIT_MODEL_PROXY_CREDENTIAL_R1",
		healthProbe: "models",
	}) as Route;

describe("route health probe", () => {
	it("asks the models listing with the route credential and never a completion", async () => {
		const seen: string[] = [];
		const base = await listen((path, auth) => {
			seen.push(`${path} ${auth}`);
			return path === "/v1/models" && auth === "Bearer key-1" ? 200 : 401;
		});
		expect(await probeModels(route(base), "key-1", 2000)).toEqual({ ok: true, status: 200 });
		expect(await probeModels(route(base), "other", 2000)).toMatchObject({ ok: false, status: 401 });
		expect(seen).toEqual(["/v1/models Bearer key-1", "/v1/models Bearer other"]);
	});

	it("marks a route unhealthy on failure and healthy again on success; unprobed routes are healthy", async () => {
		let status = 503;
		const base = await listen(() => status);
		const r = route(base);
		const cfg = { credentials: new Map([["r1", "key-1"]]) } as unknown as Config;
		const health = new RouteHealth(cfg, [r], silentLogger, 60_000, 2000);
		await health.start();
		expect(health.healthy("r1")).toBe(false);
		expect(health.healthy("unprobed")).toBe(true);
		status = 200;
		await health.probeAll();
		expect(health.healthy("r1")).toBe(true);
		health.stop();
		server?.close();
		await health.probeAll();
		// An unreachable upstream fails the probe.
		expect(health.healthy("r1")).toBe(false);
	});
});
