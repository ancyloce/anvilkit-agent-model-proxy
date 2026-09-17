import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import type { AddressInfo } from "node:net";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ControlClient } from "../src/adapters/control.js";
import { CallStore, FilesystemStore } from "../src/adapters/store.js";
import { CallService } from "../src/application/calls.js";
import { Contract } from "../src/contracts.js";
import { silentLogger } from "../src/log.js";
import { createHealthServer, createServer, MtlsIdentity } from "../src/transport/http.js";
import { checkedInConfig, configFrom, scratch } from "./helpers.js";

function openssl(args: string[], cwd: string): void {
	execFileSync("openssl", args, { cwd, stdio: "ignore" });
}

/** A private CA with a server certificate and two client certificates (one known principal, one stranger). */
function pki(dir: string): void {
	openssl(
		[
			"req",
			"-x509",
			"-newkey",
			"rsa:2048",
			"-nodes",
			"-keyout",
			"ca.key",
			"-out",
			"ca.crt",
			"-days",
			"2",
			"-subj",
			"/CN=anvilkit-dev-ca",
		],
		dir,
	);
	for (const [name, cn] of [
		["server", "localhost"],
		["workflow", "anvilkit-agent-workflow"],
		["stranger", "someone-else"],
	]) {
		writeFileSync(
			path.join(dir, `${name}.ext`),
			name === "server"
				? "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n"
				: "extendedKeyUsage=clientAuth\n",
		);
		openssl(
			["req", "-newkey", "rsa:2048", "-nodes", "-keyout", `${name}.key`, "-out", `${name}.csr`, "-subj", `/CN=${cn}`],
			dir,
		);
		openssl(
			[
				"x509",
				"-req",
				"-in",
				`${name}.csr`,
				"-CA",
				"ca.crt",
				"-CAkey",
				"ca.key",
				"-CAcreateserial",
				"-out",
				`${name}.crt`,
				"-days",
				"2",
				"-extfile",
				`${name}.ext`,
			],
			dir,
		);
	}
}

let hasOpenssl = true;
try {
	execFileSync("openssl", ["version"], { stdio: "ignore" });
} catch {
	hasOpenssl = false;
}

describe.skipIf(!hasOpenssl)("mtls identity", () => {
	const dir = scratch("anvilkit-mtls-");
	let url = "";
	let healthUrl = "";
	let close: () => Promise<void> = async () => {};

	beforeAll(async () => {
		pki(dir);
		const yaml = checkedInConfig()
			.replace("mode: disabled", "mode: mtls")
			.replace(
				"  mtls:\n    principals: []",
				`  mtls:\n    cert_file: ${path.join(dir, "server.crt")}\n    key_file: ${path.join(dir, "server.key")}\n    ca_file: ${path.join(dir, "ca.crt")}\n    principals:\n      - common_name: anvilkit-agent-workflow\n        kind: workflow`,
			)
			.replace("listen: 127.0.0.1:9103", "listen: 127.0.0.1:0");
		const cfg = configFrom(yaml, { ANVILKIT_MODEL_PROXY_STORE_DIR: scratch() });
		const contract = new Contract();
		const control = new ControlClient("127.0.0.1:1", 500, cfg.control.identity);
		const calls = new CallService({
			cfg,
			contract,
			control,
			store: new CallStore(new FilesystemStore(cfg.store.dir)),
			log: silentLogger,
			instanceId: "mtls",
		});
		const server = createServer({
			cfg,
			contract,
			calls,
			identity: new MtlsIdentity(cfg.identity.mtls.principals),
			log: silentLogger,
		});
		await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
		url = `https://127.0.0.1:${(server.address() as AddressInfo).port}`;
		const health = createHealthServer(() => true);
		await new Promise<void>((r) => health.listen(0, "127.0.0.1", r));
		healthUrl = `http://127.0.0.1:${(health.address() as AddressInfo).port}`;
		close = async () => {
			server.closeAllConnections();
			await new Promise<void>((r) => server.close(() => r()));
			health.closeAllConnections();
			await new Promise<void>((r) => health.close(() => r()));
			control.close();
		};
	});
	afterAll(async () => {
		await close();
	});

	function get(client?: string): Promise<{ status: number; body: string }> {
		return new Promise((resolve, reject) => {
			const u = new URL("/api/v1/model-calls/call_x", url);
			const req = httpsRequest(
				{
					hostname: u.hostname,
					port: u.port,
					path: u.pathname,
					method: "GET",
					ca: readFileSync(path.join(dir, "ca.crt")),
					servername: "localhost",
					...(client
						? {
								cert: readFileSync(path.join(dir, `${client}.crt`)),
								key: readFileSync(path.join(dir, `${client}.key`)),
							}
						: {}),
				},
				(res) => {
					let body = "";
					res.on("data", (c) => {
						body += c;
					});
					res.on("end", () => resolve({ status: res.statusCode ?? 0, body }));
				},
			);
			req.on("error", reject);
			req.end();
		});
	}

	it("accepts the known certificate as its principal, refuses a stranger's certificate and a connection without one", async () => {
		const known = await get("workflow");
		expect(known.status).toBe(404);
		expect(known.body).toContain("NOT_FOUND");
		const stranger = await get("stranger");
		expect(stranger.status).toBe(401);
		await expect(get()).rejects.toThrow();
	});

	it("the probes answer on the plaintext health listener without a certificate; the business listener stays mTLS", async () => {
		const probe = (pathname: string) =>
			new Promise<{ status: number; body: string }>((resolve, reject) => {
				const u = new URL(pathname, healthUrl);
				const req = httpRequest({ hostname: u.hostname, port: u.port, path: u.pathname, method: "GET" }, (res) => {
					let body = "";
					res.on("data", (c) => {
						body += c;
					});
					res.on("end", () => resolve({ status: res.statusCode ?? 0, body }));
				});
				req.on("error", reject);
				req.end();
			});
		expect(await probe("/healthz")).toEqual({ status: 200, body: "ok" });
		expect(await probe("/readyz")).toEqual({ status: 200, body: "ready" });
		expect((await probe("/api/v1/model-calls/call_x")).status).toBe(404);
		await expect(get()).rejects.toThrow();
	});
});
