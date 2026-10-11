// P1.3: process and settlement safety of the Proxy (review findings B-28,
// B-29, B-30, MP-05, MP-06, MP-07, MP-10). Every scenario counts receives
// at the countable upstream and reads the fake Control's ledger; a store
// fault is injected through a wrapper of the filesystem store.
import { connect } from "node:net";
import path from "node:path";
import { DispatchOutcome } from "@anvilkit/generated-clients/proto/anvilkit/control/v1/dispatch";
import { status } from "@grpc/grpc-js";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { type ObjectStore, PreconditionFailed, type PutOptions } from "../src/adapters/store.js";
import { jsonLogger } from "../src/log.js";
import { FakeControl } from "./control.js";
import { call, type RunningProxy, requestBody, startProxy, token } from "./proxy.js";
import { Upstream } from "./upstream.js";

/** Sends raw bytes to a listener and returns the status line (or "" when the socket closed without one). */
function rawRequest(url: string, request: string): Promise<string> {
	const u = new URL(url);
	return new Promise((resolve, reject) => {
		const socket = connect(Number(u.port), u.hostname, () => socket.write(request));
		let data = "";
		socket.setTimeout(5000, () => {
			socket.destroy();
			resolve(data.split("\r\n")[0] ?? "");
		});
		socket.on("data", (c) => {
			data += c.toString("latin1");
		});
		socket.on("end", () => resolve(data.split("\r\n")[0] ?? ""));
		socket.on("close", () => resolve(data.split("\r\n")[0] ?? ""));
		socket.on("error", reject);
	});
}

/** A store wrapper whose faults are scripted per key prefix and operation. */
function faulty(inner: ObjectStore, faults: { get: Map<string, number>; put: Map<string, number> }): ObjectStore {
	const take = (m: Map<string, number>, key: string): boolean => {
		for (const [prefix, n] of m) {
			if (key.startsWith(prefix) && n > 0) {
				m.set(prefix, n - 1);
				return true;
			}
		}
		return false;
	};
	return {
		...inner,
		get: async (k) => {
			if (take(faults.get, k)) throw new Error(`store 503 (scenario) on get ${k.split("/")[0]}`);
			return inner.get(k);
		},
		put: async (k, body, opts) => {
			if (take(faults.put, k)) throw new Error(`store timeout (scenario) on put ${k.split("/")[0]}`);
			return inner.put(k, body, opts);
		},
		delete: (k) => inner.delete(k),
		list: (p) => inner.list(p),
		qualify: () => inner.qualify(),
		describe: () => inner.describe(),
	};
}

// SAFETY_DEBUG=1 writes the Proxy's JSON log of the scenarios to stderr.
const debugLog = process.env.SAFETY_DEBUG ? jsonLogger() : undefined;
const mark = (m: string) => {
	if (debugLog) process.stderr.write(`[safety] ${m}\n`);
};

describe("P1.3 process and settlement safety", () => {
	let up: Upstream;
	let control: FakeControl;
	let proxy: RunningProxy;
	let n = 0;
	const id = (name: string) => `call_safety_${name}_${++n}_${Date.now()}`;
	const scope = (callId: string) => ({ tenantId: "tenant_a", callId });

	beforeAll(async () => {
		up = await new Upstream().start();
		control = await new FakeControl().start();
		proxy = await startProxy({ upstreamUrl: up.url, controlAddress: control.address, instanceId: "s", log: debugLog });
	});
	afterEach(() => {
		up.release();
		control.refuseObserve = undefined;
		control.refuseAdmit = undefined;
		control.unavailable = 0;
	});
	afterAll(async () => {
		await proxy.close();
		await control.stop();
		await up.stop();
	});

	it("AC1: malformed request targets and an oversized request line are answered 400 on both listeners; the process and an in-flight send are unaffected", async () => {
		up.next({ kind: "stream-then-hang", text: ["holding"] });
		const held = id("held");
		const partial = call(proxy.url, "POST", "/api/v1/model-calls", requestBody(held), token, (f) => f.length >= 2);
		await up.awaitHeld();
		const auth = `Authorization: Bearer ${token}\r\n`;
		for (const listener of [proxy.url, proxy.healthUrl]) {
			for (const target of ["//", "http://evil.example/", "*"]) {
				const line = await rawRequest(
					listener,
					`GET ${target} HTTP/1.1\r\nHost: proxy\r\n${auth}Connection: close\r\n\r\n`,
				);
				expect(line, `${listener} ${target}`).toMatch(/^HTTP\/1\.1 400/);
			}
			// A well-formed but unknown path ("/%" is a valid path to the WHATWG parser) is an ordinary 404: answered, never thrown.
			for (const target of ["/%", "/%zz"]) {
				const line = await rawRequest(
					listener,
					`GET ${target} HTTP/1.1\r\nHost: proxy\r\n${auth}Connection: close\r\n\r\n`,
				);
				expect(line, `${listener} ${target}`).toMatch(/^HTTP\/1\.1 (400|404)/);
			}
			const huge = `GET /${"a".repeat(64 * 1024)} HTTP/1.1\r\nHost: proxy\r\n${auth}Connection: close\r\n\r\n`;
			const line = await rawRequest(listener, huge);
			expect(line, `${listener} 64 KiB request line`).toMatch(/^HTTP\/1\.1 400/);
		}
		// The process serves on; the send held meanwhile ends through its own path.
		const health = await call(proxy.healthUrl, "GET", "/healthz", undefined, null);
		expect(health.status).toBe(200);
		expect(proxy.calls.inFlight).toBe(1);
		await partial;
		up.release();
		await new Promise((r) => setTimeout(r, 300));
		const rec = (await proxy.store.read(scope(held)))?.record;
		expect(rec?.state).toBe("unknown");
		expect(["USAGE_MISSING", "UPSTREAM_ERROR"]).toContain(rec?.errorCode);
	});

	it("AC2: store faults during an in-flight send never end the process: the send completes, the settlement is persisted after the store recovers, the observation reaches Control once", async () => {
		const faults = { get: new Map<string, number>(), put: new Map<string, number>() };
		const faultyProxy = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "f",
			objects: (inner) => faulty(inner, faults),
		});
		try {
			up.next({ kind: "stream-then-hang", text: ["slow"] });
			const callId = id("store-faults");
			const sends = up.receives.length;
			const partial = call(
				faultyProxy.url,
				"POST",
				"/api/v1/model-calls",
				requestBody(callId),
				token,
				(f) => f.length >= 2,
			);
			await up.awaitHeld();
			// The cancel poll reads fail for a while, the evidence and the record
			// writes fail twice: a replica-wide store outage during the send.
			faults.get.set("cancel/", 6);
			faults.put.set("evidence/", 2);
			faults.put.set("calls/", 2);
			await new Promise((r) => setTimeout(r, 400));
			up.release();
			await partial;
			// Settled once the store answers again; observed exactly once.
			const until = Date.now() + 10_000;
			let rec = (await faultyProxy.store.read(scope(callId)))?.record;
			while ((rec?.state !== "unknown" || rec.observations.some((o) => !o.submitted)) && Date.now() < until) {
				await new Promise((r) => setTimeout(r, 100));
				rec = (await faultyProxy.store.read(scope(callId)))?.record;
			}
			expect(rec?.state).toBe("unknown");
			expect(["USAGE_MISSING", "UPSTREAM_ERROR"]).toContain(rec?.errorCode);
			expect(rec?.evidenceRef).toBeDefined();
			expect(rec?.observations.every((o) => o.submitted)).toBe(true);
			expect(control.byCall(callId)?.observations).toHaveLength(1);
			expect(up.receives.length).toBe(sends + 1);
			expect(faults.get.get("cancel/")).toBe(0);
			expect(faults.put.get("evidence/")).toBe(0);
			const health = await call(faultyProxy.healthUrl, "GET", "/healthz", undefined, null);
			expect(health.status).toBe(200);
		} finally {
			await faultyProxy.close();
		}
	});

	it("AC3: a transient refusal of the observation (RESOURCE_EXHAUSTED, ABORTED, FAILED_PRECONDITION of the moment) keeps it pending; it reaches Control exactly once when accepted", async () => {
		for (const [name, code, details] of [
			["capacity", status.RESOURCE_EXHAUSTED, "CAPACITY_EXHAUSTED"],
			["revision conflict", status.ABORTED, "REVISION_CONFLICT: the operation moved"],
			["transient precondition", status.FAILED_PRECONDITION, "HOLD_PENDING: not now"],
		] as const) {
			control.refuseObserve = { code, details, times: 1 };
			up.next({ kind: "stream", text: ["observed later"] });
			const callId = id(`observe-${name.replace(/ /g, "-")}`);
			const r = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(callId));
			expect(r.status, name).toBe(200);
			expect(r.frames.at(-1)?.type, name).toBe("done");
			const until = Date.now() + 5000;
			let rec = (await proxy.store.read(scope(callId)))?.record;
			while (rec?.observations.some((o) => !o.submitted) && Date.now() < until) {
				await new Promise((res) => setTimeout(res, 50));
				rec = (await proxy.store.read(scope(callId)))?.record;
			}
			expect(
				rec?.observations.every((o) => o.submitted),
				name,
			).toBe(true);
			const d = control.byCall(callId);
			expect(d?.observations, name).toHaveLength(1);
			expect(d?.outcome, name).toBe(DispatchOutcome.DISPATCH_OUTCOME_SUCCEEDED);
			expect(control.observes.filter((o) => o.dispatchId === d?.dispatchId).length, name).toBe(2);
		}
		// A permanent answer about the identity ends the observation: no retry.
		control.refuseObserve = { code: status.NOT_FOUND, details: "NOT_FOUND: dispatch gone", times: 1 };
		up.next({ kind: "stream", text: ["gone"] });
		const gone = id("observe-not-found");
		await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(gone));
		await new Promise((res) => setTimeout(res, 400));
		const rec = (await proxy.store.read(scope(gone)))?.record;
		expect(rec?.observations.every((o) => o.submitted)).toBe(true);
		expect(control.byCall(gone)?.observations).toHaveLength(0);
	});

	it("AC4: admission under Control capacity exhaustion is answered retryable (503); the same call id is admitted once capacity returns, one send", async () => {
		control.refuseAdmit = { code: status.RESOURCE_EXHAUSTED, details: "CAPACITY_EXHAUSTED", times: 3 };
		const callId = id("capacity");
		const sends = up.receives.length;
		const refused = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(callId));
		expect(refused.status).toBe(503);
		expect((refused.json as { error: { code: string; retryable: boolean } }).error).toMatchObject({
			code: "DEPENDENCY_UNAVAILABLE",
			retryable: true,
		});
		expect(up.receives.length).toBe(sends);
		expect(await proxy.store.read(scope(callId))).toBeUndefined();
		// A budget refusal under the same gRPC status stays a refusal of the call.
		control.refuseAdmit = { code: status.RESOURCE_EXHAUSTED, details: "BUDGET_EXHAUSTED: no allocation", times: 1 };
		const broke = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(id("budget")));
		expect(broke.status).toBe(429);
		expect((broke.json as { error: { code: string } }).error.code).toBe("BUDGET_EXHAUSTED");
		control.refuseAdmit = undefined;
		up.next({ kind: "stream", text: ["admitted now"] });
		const ok = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(callId));
		expect(ok.status).toBe(200);
		expect(ok.frames.at(-1)?.type).toBe("done");
		expect(up.receives.length).toBe(sends + 1);
	});

	it("drain: a closing Proxy refuses new opens as retryable, answers reentries and queries, and lets an in-flight send settle", async () => {
		const draining = await startProxy({ upstreamUrl: up.url, controlAddress: control.address, instanceId: "dr" });
		up.next({ kind: "stream-then-hang", text: ["in flight"] });
		const held = id("drain-held");
		const heldBody = requestBody(held);
		const sends = up.receives.length;
		const partial = call(draining.url, "POST", "/api/v1/model-calls", heldBody, token, (f) => f.length >= 2);
		await up.awaitHeld();
		draining.calls.beginClose();
		const refused = await call(draining.url, "POST", "/api/v1/model-calls", requestBody(id("drain-new")));
		expect(refused.status).toBe(503);
		expect((refused.json as { error: { retryable: boolean } }).error.retryable).toBe(true);
		const reentry = call(draining.url, "POST", "/api/v1/model-calls", heldBody, token, (f) => f.length >= 2);
		expect((await reentry).status).toBe(200);
		const q = await call(draining.url, "GET", `/api/v1/model-calls/${held}`);
		expect(q.status).toBe(200);
		await partial;
		const closing = draining.calls.close();
		up.release();
		await closing;
		const rec = (await draining.store.read(scope(held)))?.record;
		expect(rec?.state).toBe("unknown");
		expect(["USAGE_MISSING", "UPSTREAM_ERROR"]).toContain(rec?.errorCode);
		expect(rec?.errorCode).not.toBe("SENDER_LOST");
		expect(up.receives.length).toBe(sends + 1);
		await draining.close();
	});

	it("AC6: usage is parsed from the whole stream, not the evidence capture; counters of a stream cut before its end settle unknown/USAGE_INCOMPLETE, never a definite outcome", async () => {
		// A route whose evidence bound is far below the body: the evidence is
		// truncated, the stream completes, the usage is exact.
		const small = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "ev",
			log: debugLog,
			configEdits: (yaml) => yaml.replace("max_evidence_bytes: 4194304", "max_evidence_bytes: 1024"),
		});
		try {
			mark("AC6 small proxy started");
			const big = Array.from({ length: 200 }, (_, i) => `chunk ${i} of a long answer `);
			up.next({ kind: "stream", text: big, usage: { prompt_tokens: 7, completion_tokens: 400 } });
			const callId = id("evidence-truncated");
			mark("AC6 first call");
			const r = await call(small.url, "POST", "/api/v1/model-calls", requestBody(callId));
			mark(`AC6 first call answered ${r.status} with ${r.frames.length} frames`);
			expect(r.status).toBe(200);
			expect(r.frames.at(-1)).toMatchObject({
				type: "done",
				outcome: "succeeded",
				usage: { inputUnits: "7", outputUnits: "400" },
			});
			const ev = JSON.parse(
				(await import("node:fs")).readFileSync(path.join(small.storeDir, "evidence", callId, "tenant_a"), "utf8"),
			) as { response: { bodyTruncated: boolean; bodyBytes: number } };
			expect(ev.response.bodyTruncated).toBe(true);
			expect(ev.response.bodyBytes).toBeGreaterThan(1024);
			// Counters that arrived on a stream cut before its end are a lower bound.
			up.next({
				kind: "stream-then-hang",
				text: ["partial"],
				usageBeforeHang: { prompt_tokens: 7, completion_tokens: 1 },
			});
			const cut = id("usage-incomplete");
			const partial = await call(
				small.url,
				"POST",
				"/api/v1/model-calls",
				requestBody(cut),
				token,
				(f) => f.length >= 2,
			);
			expect(partial.status).toBe(200);
			await up.awaitHeld();
			mark("AC6 held; cancelling");
			const cancel = await call(small.url, "POST", `/api/v1/model-calls/${cut}/cancellations`, "{}");
			expect(cancel.status).toBe(202);
			mark("AC6 cancelled; releasing");
			up.release();
			const until = Date.now() + 5000;
			let rec = (await small.store.read(scope(cut)))?.record;
			while (rec?.state === "sending" && Date.now() < until) {
				await new Promise((res) => setTimeout(res, 50));
				rec = (await small.store.read(scope(cut)))?.record;
			}
			expect(rec?.state).toBe("unknown");
			expect(rec?.errorCode).toBe("CANCELED");
			expect(rec?.usage).toBeUndefined();
			const d = control.byCall(cut);
			expect(d?.observations[0]?.outcome).toBe(DispatchOutcome.DISPATCH_OUTCOME_UNKNOWN);
			expect(d?.observations[0]?.usage).toBeUndefined();
			mark("AC6 assertions done; closing");
		} finally {
			await small.close();
			mark("AC6 closed");
		}
	});

	it("AC6b: a stream that ends after its usage but without the terminator, or whose connection is cut right after the counters, is USAGE_INCOMPLETE", async () => {
		up.next({
			kind: "stream-then-hang",
			text: ["almost"],
			usageBeforeHang: { prompt_tokens: 3, completion_tokens: 2 },
		});
		const callId = id("cut-after-usage");
		const sends = up.receives.length;
		const partial = call(proxy.url, "POST", "/api/v1/model-calls", requestBody(callId), token, (f) => f.length >= 2);
		await up.awaitHeld();
		await partial;
		// The connection is destroyed by the upstream (not ended): the stream never completed.
		up.destroyHeld();
		const until = Date.now() + 5000;
		let rec = (await proxy.store.read(scope(callId)))?.record;
		while (rec?.state === "sending" && Date.now() < until) {
			await new Promise((res) => setTimeout(res, 50));
			rec = (await proxy.store.read(scope(callId)))?.record;
		}
		expect(rec?.state).toBe("unknown");
		expect(["USAGE_INCOMPLETE", "UPSTREAM_ERROR"]).toContain(rec?.errorCode);
		expect(rec?.usage).toBeUndefined();
		expect(up.receives.length).toBe(sends + 1);
	});

	it("AC7: a thousand reclaimed watches are read in bounded pages; a sweep pass stays bounded and readiness does not depend on it", async () => {
		const seeded = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "rc",
			configEdits: (yaml) => yaml.replace("  sweep_batch: 200", "  sweep_batch: 100"),
		});
		try {
			for (let i = 0; i < 1000; i++)
				await seeded.store.markReclaimed({ tenantId: "tenant_a", callId: `reclaimed_${i}` });
			const t0 = Date.now();
			const first = await seeded.calls.sweep();
			const elapsed = Date.now() - t0;
			expect(first.reclaimed).toBe(0);
			expect(elapsed).toBeLessThan(2000);
			const reads = (await seeded.store.listReclaimed()).length;
			expect(reads).toBe(1000);
			// Readiness is a flag of the bootstrap, flipped before the first sweep (main.ts); the probe listener answers meanwhile.
			const health = await call(seeded.healthUrl, "GET", "/readyz", undefined, null);
			expect(health.status).toBe(200);
		} finally {
			await seeded.close();
		}
	});

	it("AC8: a conditional create whose answer was lost and that the SDK repeated is recognised as the holder's own record: it proceeds to send once", async () => {
		let lostOnce = false;
		const wrap = (inner: ObjectStore): ObjectStore => ({
			...inner,
			get: (k) => inner.get(k),
			delete: (k) => inner.delete(k),
			list: (p) => inner.list(p),
			qualify: () => inner.qualify(),
			describe: () => inner.describe(),
			put: async (key: string, body: Uint8Array, opts?: PutOptions) => {
				const version = await inner.put(key, body, opts);
				if (!lostOnce && key.startsWith("calls/") && opts?.ifNoneMatch) {
					// The write landed; the SDK retried the PUT and the backend
					// answered the retry with a precondition failure.
					lostOnce = true;
					throw new PreconditionFailed(`${key}: PreconditionFailed (retried PUT)`);
				}
				return version;
			},
		});
		const holder = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "own",
			objects: wrap,
		});
		try {
			up.next({ kind: "stream", text: ["own write"] });
			const callId = id("own-write");
			const sends = up.receives.length;
			const r = await call(holder.url, "POST", "/api/v1/model-calls", requestBody(callId));
			expect(r.status, r.text).toBe(200);
			expect(r.frames.map((f) => f.type)).toEqual(["admitted", "text", "usage", "done"]);
			expect(up.receives.length).toBe(sends + 1);
			expect(lostOnce).toBe(true);
			const rec = (await holder.store.read(scope(callId)))?.record;
			expect(rec).toMatchObject({ state: "succeeded", createdBy: "model-proxy/own" });
			expect(control.byCall(callId)?.observations).toHaveLength(1);
		} finally {
			await holder.close();
		}
	});

	it("retention: terminal records, their evidence and cancel intents older than their retention are removed by a bounded sweep; a watched reclaimed record is kept", async () => {
		let now = Date.now();
		const aged = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "rt",
			now: () => now,
			configEdits: (yaml) =>
				yaml
					.replace("  retention_calls: 168h", "  retention_calls: 25h")
					.replace("  retention_evidence: 720h", "  retention_evidence: 26h")
					.replace("  retention_cancel: 48h", "  retention_cancel: 2h"),
		});
		try {
			up.next({ kind: "stream", text: ["to be retained"] });
			const callId = id("retained");
			const r = await call(
				aged.url,
				"POST",
				"/api/v1/model-calls",
				requestBody(callId, { deadline: new Date(now + 60_000).toISOString() }),
			);
			expect(r.status).toBe(200);
			const cancel = await call(aged.url, "POST", `/api/v1/model-calls/${callId}/cancellations`, "{}");
			expect(cancel.status).toBe(202);
			await aged.store.markReclaimed({ tenantId: "tenant_a", callId: "watched_forever" });
			let pass = await aged.calls.sweep();
			expect(pass.expired).toBe(0);
			now += 3 * 3_600_000;
			pass = await aged.calls.sweep();
			expect(pass.expired).toBe(1); // the cancel intent
			expect(await aged.store.cancelRequested(scope(callId))).toBeUndefined();
			now += 22.5 * 3_600_000; // 25.5h: past the record's retention, before the evidence's
			pass = await aged.calls.sweep();
			expect(pass.expired).toBe(1); // the record
			expect(await aged.store.read(scope(callId))).toBeUndefined();
			expect(await aged.store.readEvidence(scope(callId))).toBeDefined();
			now += 2 * 3_600_000;
			pass = await aged.calls.sweep();
			expect(pass.expired).toBe(1); // the evidence
			expect(await aged.store.readEvidence(scope(callId))).toBeUndefined();
			expect((await aged.store.listReclaimed()).map((s) => s.callId)).toContain("watched_forever");
		} finally {
			await aged.close();
		}
	});
});
