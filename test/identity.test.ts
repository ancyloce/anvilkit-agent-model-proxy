// P0.1 Control transport of the Model Proxy: the workload certificate is
// presented, Control is verified by its server name through standard
// hostname verification, rotation follows the files, and the configuration
// loader admits plaintext or bearer identities only under the guard.

import { DispatchServiceService } from "@anvilkit/generated-clients/proto/anvilkit/control/v1/dispatch";
import { Server, ServerCredentials, status } from "@grpc/grpc-js";
import { describe, expect, it } from "vitest";
import { ControlClient, ControlRefused, ControlUnavailable } from "../src/adapters/control.js";
import { checkedInConfig, configFrom } from "./helpers.js";
import { type CA, files, issue, type Leaf, mount, newCA, spiffe, tempDir } from "./pki.js";

const proxyURI = spiffe("anvilkit.local", "anvilkit-apps", "anvilkit-agent-control");
const controlLeaf = (ca: CA) =>
	issue(
		ca,
		"anvilkit-agent-control",
		[spiffe("anvilkit.local", "anvilkit-apps", "anvilkit-agent-control")],
		["anvilkit-agent-control"],
	);

/** A Control double answering NOT_FOUND under the given bundle and leaf. */
async function control(caPem: Buffer, leaf: Leaf): Promise<{ address: string; stop(): void }> {
	const server = new Server();
	server.addService(DispatchServiceService, {
		getDispatch: (_c: unknown, cb: (e: Error, r: null) => void) =>
			cb(
				Object.assign(new Error("NOT_FOUND: no such dispatch"), {
					code: status.NOT_FOUND,
					details: "NOT_FOUND: no such dispatch",
				}),
				null,
			),
		admitModel: () => {},
		observeDispatch: () => {},
		admitTool: () => {},
		confirmNotSent: () => {},
	});
	const port = await new Promise<number>((resolve, reject) =>
		server.bindAsync(
			"127.0.0.1:0",
			ServerCredentials.createSsl(caPem, [{ private_key: leaf.keyPem, cert_chain: leaf.certPem }], true),
			(e, p) => (e ? reject(e) : resolve(p)),
		),
	);
	return { address: `127.0.0.1:${port}`, stop: () => server.forceShutdown() };
}

async function probe(address: string, dir: string, serverName = "anvilkit-agent-control"): Promise<string> {
	const f = files(dir);
	const c = new ControlClient(address, 2000, { mode: "mtls", mtls: { ...f, serverName } });
	try {
		await c.getDispatch("tenant_a", "dsp_missing");
		return "ok";
	} catch (err) {
		if (err instanceof ControlRefused) return err.code;
		if (err instanceof ControlUnavailable) return "unavailable";
		return String(err);
	} finally {
		c.close();
	}
}

describe("Control transport identity", () => {
	it("presents the workload certificate, verifies Control by server name and follows rotation", async () => {
		const ca1 = newCA("ca1");
		const ca2 = newCA("ca2");
		const dir = tempDir();
		mount(dir, issue(ca1, "anvilkit-agent-model-proxy", [proxyURI]), ca1.pem);
		const c1 = await control(ca1.pem, controlLeaf(ca1));
		expect(await probe(c1.address, dir)).toBe("NOT_FOUND");
		expect(await probe(c1.address, dir, "anvilkit-agent-mcp"), "hostname verification").toBe("unavailable");
		const foreign = await control(ca1.pem, controlLeaf(ca2));
		expect(await probe(foreign.address, dir), "a Control of an untrusted CA").toBe("unavailable");
		foreign.stop();
		// leaf replaced under the same CA, then the transition bundle, then ca1 retired
		mount(dir, issue(ca1, "anvilkit-agent-model-proxy", [proxyURI]), ca1.pem);
		expect(await probe(c1.address, dir)).toBe("NOT_FOUND");
		mount(dir, issue(ca1, "anvilkit-agent-model-proxy", [proxyURI]), Buffer.concat([ca1.pem, ca2.pem]));
		const c2 = await control(Buffer.concat([ca1.pem, ca2.pem]), controlLeaf(ca2));
		expect(await probe(c2.address, dir)).toBe("NOT_FOUND");
		expect(await probe(c1.address, dir)).toBe("NOT_FOUND");
		mount(dir, issue(ca2, "anvilkit-agent-model-proxy", [proxyURI]), ca2.pem);
		expect(await probe(c1.address, dir), "retired CA").toBe("unavailable");
		expect(await probe(c2.address, dir)).toBe("NOT_FOUND");
		c1.stop();
		c2.stop();
	});

	it("admits the bearer listener identity and the plaintext Control transport only under the guard", () => {
		const base = checkedInConfig();
		const env = { ANVILKIT_MODEL_PROXY_PRINCIPALS_FILE: "/dev/null", ANVILKIT_MODEL_PROXY_STORE_DIR: "/tmp/x" };
		const unguarded = base.replace("development:\n  enabled: true\n", "development:\n  enabled: false\n");
		expect(() => configFrom(unguarded.replace("mode: disabled", "mode: development"), env)).toThrow(
			/identity.mode development .*requires development.enabled/,
		);
		expect(() =>
			configFrom(
				unguarded
					.replace("mode: disabled", "mode: development")
					.replace(
						"  identity:\n    mode: mtls\n    mtls:\n      server_name: anvilkit-agent-control\n",
						"  identity:\n    mode: development\n",
					),
				env,
			),
		).toThrow(/control.identity.mode development .*requires development.enabled/);
		const ca = newCA("ca");
		const dir = tempDir();
		mount(dir, issue(ca, "anvilkit-agent-model-proxy", [proxyURI]), ca.pem);
		const f = files(dir);
		const cfg = configFrom(
			unguarded
				.replace("mode: disabled", "mode: mtls")
				.replace(
					"  mtls:\n    principals: []",
					`  mtls:\n    cert_file: ${f.certFile}\n    key_file: ${f.keyFile}\n    ca_file: ${f.caFile}\n    principals:\n      - spiffe_id: spiffe://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-workflow\n        principal_id: anvilkit-agent-workflow\n        kind: workflow`,
				),
			{
				ANVILKIT_MODEL_PROXY_STORE_DIR: "/tmp/x",
				ANVILKIT_MODEL_PROXY_CONTROL_IDENTITY_CERT_FILE: f.certFile,
				ANVILKIT_MODEL_PROXY_CONTROL_IDENTITY_KEY_FILE: f.keyFile,
				ANVILKIT_MODEL_PROXY_CONTROL_IDENTITY_CA_FILE: f.caFile,
			},
		);
		expect(cfg.development.enabled).toBe(false);
		expect(cfg.control.identity.mode).toBe("mtls");
		expect(cfg.control.identity.mtls.serverName).toBe("anvilkit-agent-control");
		expect(() => configFrom(base, { ...env, ANVILKIT_MODEL_PROXY_DEVELOPMENT_ENABLED: "true" })).toThrow();
	});
});
