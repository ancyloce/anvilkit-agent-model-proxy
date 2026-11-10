import { execFileSync } from "node:child_process";
import { X509Certificate } from "node:crypto";
import { once } from "node:events";
import { readFileSync, writeFileSync } from "node:fs";
import { request as httpRequest, type IncomingMessage } from "node:http";
import { Agent, request as httpsRequest } from "node:https";
import type { AddressInfo } from "node:net";
import path from "node:path";
import type { TLSSocket } from "node:tls";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ControlClient } from "../src/adapters/control.js";
import { CallStore, FilesystemStore } from "../src/adapters/store.js";
import { CallService } from "../src/application/calls.js";
import { Contract } from "../src/contracts.js";
import { IdentityWatcher } from "../src/identity.js";
import {
	createHealthServer,
	createServer,
	currentTrustOnly,
	MtlsIdentity,
	parseSubjectAltName,
	spiffeIdOf,
} from "../src/transport/http.js";
import { checkedInConfig, configFrom, scratch, silentLogger } from "./helpers.js";
import { type CA, files, issue, mount, newCA, tempDir } from "./pki.js";

function openssl(args: string[], cwd: string): void {
	execFileSync("openssl", args, { cwd, stdio: "ignore" });
}

const workflowId = "spiffe://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-workflow";

/**
 * The client certificates by name: [common name, subjectAltName]. Only the
 * SPIFFE ID (the single URI SAN) authenticates (F-P0.1-1); the common name is
 * never consulted.
 */
const clients: Record<string, [string, string]> = {
	workflow: ["anvilkit-agent-workflow", `URI:${workflowId}`],
	renamed: ["not-the-workflow", `URI:${workflowId},DNS:anything.example`],
	stranger: ["someone-else", "URI:spiffe://anvilkit.local/ns/anvilkit-apps/sa/someone-else"],
	"cn-only": ["anvilkit-agent-workflow", "DNS:anvilkit-agent-workflow"],
	"other-uri": ["anvilkit-agent-workflow", "URI:https://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-workflow"],
	"two-spiffe": [
		"anvilkit-agent-workflow",
		`URI:${workflowId},URI:spiffe://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-control`,
	],
};

/** A private CA with a server certificate and the client certificates above. */
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
	for (const [name, cn, san] of [
		["server", "localhost", "IP:127.0.0.1,DNS:localhost"],
		...Object.entries(clients).map(([name, [cn, san]]) => [name, cn, san]),
	] as [string, string, string][]) {
		writeFileSync(
			path.join(dir, `${name}.ext`),
			`subjectAltName=${san}\nextendedKeyUsage=${name === "server" ? "serverAuth" : "clientAuth"}\n`,
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
				`  mtls:\n    cert_file: ${path.join(dir, "server.crt")}\n    key_file: ${path.join(dir, "server.key")}\n    ca_file: ${path.join(dir, "ca.crt")}\n    principals:\n      - spiffe_id: ${workflowId}\n        principal_id: anvilkit-agent-workflow\n        kind: workflow`,
			)
			.replace("listen: 127.0.0.1:9103", "listen: 127.0.0.1:0")
			.replace(
				"  identity:\n    mode: mtls\n    mtls:\n      server_name: anvilkit-agent-control\n",
				"  identity:\n    mode: development\n",
			);
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

	it("authenticates by the SPIFFE ID only: the right common name without it, another URI or two SPIFFE IDs are refused (F-P0.1-1)", async () => {
		expect((await get("renamed")).status).toBe(404);
		for (const name of ["cn-only", "other-uri", "two-spiffe"]) expect((await get(name)).status, name).toBe(401);
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

describe("SPIFFE principal mapping (F-P0.1-1)", () => {
	const socketWith = (subjectaltname: string | undefined, authorized = true, cn = "anvilkit-agent-workflow") =>
		({
			socket: { authorized, getPeerCertificate: () => ({ subject: { CN: cn }, subjectaltname }) },
		}) as unknown as IncomingMessage;
	const sidecarId = "spiffe://anvilkit.local/ns/anvilkit-jobs/sa/anvilkit-job-access-sidecar";
	const identity = new MtlsIdentity([
		{ spiffeId: workflowId, principalId: "workflow-principal", kind: "workflow" },
		{ spiffeId: sidecarId, principalId: "sidecar-a", kind: "sidecar" },
	]);

	it("returns the configured principal id and kind of the certificate's SPIFFE ID", () => {
		expect(
			identity.authenticate(socketWith(`DNS:localhost, URI:${workflowId}, IP Address:127.0.0.1`, true, "x")),
		).toEqual({
			id: "workflow-principal",
			kind: "workflow",
		});
		expect(identity.authenticate(socketWith(`URI:${sidecarId}`))).toEqual({ id: "sidecar-a", kind: "sidecar" });
	});

	it("refuses an unauthorized peer, no or another URI, several URIs and an unknown or malformed list", () => {
		for (const san of [
			undefined,
			"",
			"DNS:anvilkit-agent-workflow",
			"URI:https://anvilkit.local/workflow",
			`URI:${workflowId}, URI:${workflowId}`,
			`URI:${workflowId}, URI:${sidecarId}`,
			`URI:${workflowId}, URI:https://anvilkit.local/x`,
			"URI:spiffe://anvilkit.local/ns/anvilkit-apps/sa/unknown",
			`URI:${workflowId}, `,
			`URI:"${workflowId}`,
		])
			expect(identity.authenticate(socketWith(san)), String(san)).toBeUndefined();
		expect(identity.authenticate(socketWith(`URI:${workflowId}`, false))).toBeUndefined();
	});

	it("parses quoted values with commas and escapes as Node prints them", () => {
		expect(
			parseSubjectAltName('DNS:a.example, URI:"spiffe://anvilkit.local/x, URI:spiffe://evil/y", IP Address:10.0.0.1'),
		).toEqual([
			{ type: "DNS", value: "a.example" },
			{ type: "URI", value: "spiffe://anvilkit.local/x, URI:spiffe://evil/y" },
			{ type: "IP Address", value: "10.0.0.1" },
		]);
		expect(parseSubjectAltName(String.raw`URI:"a\"b\\c"`)).toEqual([{ type: "URI", value: String.raw`a"b\c` }]);
		// A comma inside a quoted value never yields a second SPIFFE ID.
		expect(spiffeIdOf('URI:"spiffe://anvilkit.local/x, URI:spiffe://evil/y"')).toBe(
			"spiffe://anvilkit.local/x, URI:spiffe://evil/y",
		);
		expect(spiffeIdOf(`DNS:x, URI:${workflowId}`)).toBe(workflowId);
		expect(parseSubjectAltName("no-colon")).toBeUndefined();
		expect(parseSubjectAltName("DNS:a,DNS:b")).toEqual([{ type: "DNS", value: "a,DNS:b" }]);
	});
});

describe.skipIf(!hasOpenssl)("mtls trust rotation", () => {
	const proxyId = "spiffe://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-model-proxy";

	/** One GET on the agent; the status and the socket that carried it. */
	function getOn(agent: Agent, base: string): Promise<{ status: number; socket: TLSSocket; reused: boolean }> {
		return new Promise((resolve, reject) => {
			const u = new URL("/api/v1/model-calls/call_x", base);
			let socket: TLSSocket | undefined;
			const req = httpsRequest(
				{ hostname: u.hostname, port: u.port, path: u.pathname, method: "GET", agent, servername: "localhost" },
				(res) => {
					res.resume();
					res.on("end", () =>
						resolve({ status: res.statusCode ?? 0, socket: socket as TLSSocket, reused: req.reusedSocket }),
					);
				},
			);
			req.on("socket", (s) => {
				socket = s as TLSSocket;
			});
			req.on("error", reject);
			req.end();
		});
	}

	it("authenticates a request only while its connection's certificate is issued by a current CA", () => {
		const ca1 = newCA("ca1");
		const ca2 = newCA("ca2");
		const leaf = new X509Certificate(issue(ca1, "anvilkit-agent-workflow", [workflowId]).certPem);
		const request = {
			socket: {
				authorized: true,
				getPeerX509Certificate: () => leaf,
				getPeerCertificate: () => ({ subjectaltname: `URI:${workflowId}` }),
			},
		} as unknown as IncomingMessage;
		let bundle = [new X509Certificate(ca1.pem), new X509Certificate(ca2.pem)];
		const identity = currentTrustOnly(
			new MtlsIdentity([{ spiffeId: workflowId, principalId: "workflow-principal", kind: "workflow" }]),
			() => bundle,
		);
		expect(identity.authenticate(request)).toEqual({ id: "workflow-principal", kind: "workflow" });
		bundle = [new X509Certificate(ca2.pem)];
		expect(identity.authenticate(request), "ca1 retired, the handshake's verdict notwithstanding").toBeUndefined();
		const noCertificate = { socket: { authorized: true, getPeerX509Certificate: () => undefined } };
		expect(identity.authenticate(noCertificate as unknown as IncomingMessage)).toBeUndefined();
	});

	it("closes the connections a retired CA had verified, keeps the trusted ones and stops following the watcher once closed", async () => {
		const ca1 = newCA("ca1");
		const ca2 = newCA("ca2");
		const both = Buffer.concat([ca1.pem, ca2.pem]);
		const dir = tempDir();
		mount(dir, issue(ca1, "localhost", [proxyId]), both);
		const f = files(dir);
		const cfg = configFrom(
			checkedInConfig()
				.replace("mode: disabled", "mode: mtls")
				.replace(
					"  mtls:\n    principals: []",
					`  mtls:\n    cert_file: ${f.certFile}\n    key_file: ${f.keyFile}\n    ca_file: ${f.caFile}\n    principals:\n      - spiffe_id: ${workflowId}\n        principal_id: anvilkit-agent-workflow\n        kind: workflow`,
				)
				.replace(
					"  identity:\n    mode: mtls\n    mtls:\n      server_name: anvilkit-agent-control\n",
					"  identity:\n    mode: development\n",
				),
			{ ANVILKIT_MODEL_PROXY_STORE_DIR: scratch() },
		);
		const watcher = new IdentityWatcher(f, 60_000);
		const control = new ControlClient("127.0.0.1:1", 500, cfg.control.identity);
		const logged: string[] = [];
		const server = createServer({
			cfg,
			contract: new Contract(),
			calls: new CallService({
				cfg,
				contract: new Contract(),
				control,
				store: new CallStore(new FilesystemStore(cfg.store.dir)),
				log: silentLogger,
				instanceId: "rotation",
			}),
			identity: new MtlsIdentity(cfg.identity.mtls.principals),
			identityWatcher: watcher,
			log: { ...silentLogger, info: (msg) => logged.push(msg) },
		});
		// Idle kept-alive connections stay open for the whole test: only the
		// rotation closes one.
		server.keepAliveTimeout = 60_000;
		const accepted: TLSSocket[] = [];
		server.on("secureConnection", (socket: TLSSocket) => accepted.push(socket));
		await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
		const base = `https://127.0.0.1:${(server.address() as AddressInfo).port}`;
		const clientOf = (ca: CA) => {
			const leaf = issue(ca, "anvilkit-agent-workflow", [workflowId]);
			return new Agent({ keepAlive: true, maxSockets: 1, ca: both, cert: leaf.certPem, key: leaf.keyPem });
		};
		const retiring = clientOf(ca1);
		const staying = clientOf(ca2);
		try {
			const before = await getOn(retiring, base);
			expect(before.status, "a ca1 client during the transition").toBe(404);
			const kept = await getOn(staying, base);
			expect(kept.status, "a ca2 client during the transition").toBe(404);

			// ca1 retired: the server leaf moves to ca2 and the bundle drops ca1.
			const closed = once(before.socket, "close");
			mount(dir, issue(ca2, "localhost", [proxyId]), ca2.pem);
			expect(watcher.reload()).toBe(true);
			expect(
				accepted.map((a) => a.destroyed),
				"the ca1 connection is closed at the rotation, the ca2 one kept",
			).toEqual([true, false]);
			await closed;
			expect(logged).toContain("closed connections of retired trust");
			expect(kept.socket.destroyed, "a connection the current bundle still trusts stays open").toBe(false);
			const again = await getOn(staying, base);
			expect(again.status).toBe(404);
			expect(again.reused, "served on the kept connection").toBe(true);

			// A new connection of the retired CA (a full handshake or a resumed
			// session) authenticates nothing.
			const retried = await getOn(retiring, base).then(
				(r) => r.status,
				() => "refused",
			);
			expect([401, "refused"]).toContain(retried);
		} finally {
			retiring.destroy();
			staying.destroy();
			server.closeAllConnections();
			await new Promise<void>((r) => server.close(() => r()));
			control.close();
		}
		// Closed: the listener no longer follows the watcher.
		let followed = false;
		(server as unknown as { setSecureContext: () => void }).setSecureContext = () => {
			followed = true;
		};
		mount(dir, issue(ca2, "localhost", [proxyId]), both);
		expect(watcher.reload()).toBe(true);
		expect(followed).toBe(false);
	});
});
