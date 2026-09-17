import { describe, expect, it } from "vitest";
import { CallStore, FilesystemStore, type ObjectStore, PreconditionFailed, S3Store } from "../src/adapters/store.js";
import { scratch } from "./helpers.js";

const enc = (s: string) => new TextEncoder().encode(s);

function semantics(name: string, make: () => Promise<ObjectStore>) {
	describe(name, () => {
		it("creates conditionally, replaces by version, lists completely and qualifies", async () => {
			const store = await make();
			await store.qualify();
			const key = `calls/store-${Date.now()}`;
			const v1 = await store.put(key, enc('{"n":1}'), { ifNoneMatch: true });
			await expect(store.put(key, enc('{"n":2}'), { ifNoneMatch: true })).rejects.toBeInstanceOf(PreconditionFailed);
			expect(new TextDecoder().decode((await store.get(key))?.body)).toBe('{"n":1}');
			await expect(store.put(key, enc('{"n":2}'), { ifMatch: "not-the-version" })).rejects.toBeInstanceOf(
				PreconditionFailed,
			);
			const v2 = await store.put(key, enc('{"n":2}'), { ifMatch: v1 });
			expect(v2).not.toBe(v1);
			expect((await store.get(key))?.version).toBe(v2);
			await expect(store.put(key, enc('{"n":3}'), { ifMatch: v1 })).rejects.toBeInstanceOf(PreconditionFailed);
			expect(await store.list("calls/")).toContain(key);
			await store.delete(key);
			expect(await store.get(key)).toBeUndefined();
			expect(await store.list("calls/")).not.toContain(key);
			await expect(store.get("../etc/passwd")).rejects.toThrow(/invalid store key/);
			await expect(store.put("calls/..", enc("x"))).rejects.toThrow(/invalid store key/);
		});

		it("keeps evidence immutable, pending markers and cancel intents idempotent, every object under the call's scope", async () => {
			const calls = new CallStore(await make());
			const callId = `call_${Date.now()}`;
			const scope = { tenantId: "tenant_a", callId };
			const other = { tenantId: "tenant_b", callId };
			const evidence = {
				callId,
				tenantId: "tenant_a",
				dispatchId: "d",
				routeId: "r",
				capturedAt: "2026-09-16T00:00:00Z",
				request: { method: "POST", url: "u", contentType: "", body: "first", bodyTruncated: false },
				settlement: { outcome: "unknown" as const, errorCode: "UPSTREAM_ERROR", frames: [] },
			};
			expect(await calls.writeEvidence(evidence)).toBe(`evidence/${callId}/tenant_a`);
			await calls.writeEvidence({ ...evidence, request: { ...evidence.request, body: "second" } });
			expect((await calls.readEvidence(scope))?.request.body).toBe("first");
			expect((await calls.readEvidence(scope))?.settlement.errorCode).toBe("UPSTREAM_ERROR");
			// Another tenant's call under the same id has objects of its own.
			expect(await calls.readEvidence(other)).toBeUndefined();
			await calls.writeEvidence({ ...evidence, tenantId: "tenant_b", request: { ...evidence.request, body: "b" } });
			expect((await calls.readEvidence(other))?.request.body).toBe("b");
			expect((await calls.readEvidence(scope))?.request.body).toBe("first");
			await calls.markPending(scope, "2026-09-16T00:01:00Z");
			await calls.markPending(scope, "2026-09-16T00:02:00Z");
			expect(await calls.listPending()).toContainEqual(scope);
			expect(await calls.listPending()).not.toContainEqual(other);
			expect(await calls.readPending(scope)).toEqual({ deadline: "2026-09-16T00:01:00Z" });
			await calls.clearPending(scope);
			expect(await calls.listPending()).not.toContainEqual(scope);
			expect(await calls.cancelRequested(scope)).toBeUndefined();
			await calls.requestCancel(scope, "2026-09-16T00:00:01Z");
			await calls.requestCancel(scope, "2026-09-16T00:00:02Z");
			expect(await calls.cancelRequested(scope)).toBe("2026-09-16T00:00:01Z");
			expect(await calls.cancelRequested(other)).toBeUndefined();
			// A query by call id alone sees the scopes recorded under it, and nothing of another id.
			const record = {
				callId,
				tenantId: "tenant_a",
				principalId: "p",
				routeId: "r",
				provider: "x",
				model: "y",
				requestDigest: "d",
				contentDigest: "c",
				binding: { tenantId: "tenant_a", operationId: "o", attemptId: "a", executionEpoch: "1" },
				deadline: "2026-09-16T00:01:00Z",
				maxExposure: { currency: "USD", amount: "1" },
				maxOutputTokens: 1,
				dispatchId: "d",
				state: "sending" as const,
				createdAt: "2026-09-16T00:00:00Z",
				updatedAt: "2026-09-16T00:00:00Z",
				revision: 1,
				frames: [],
				observations: [],
			};
			await calls.create(record);
			await calls.create({ ...record, tenantId: "tenant_b", binding: { ...record.binding, tenantId: "tenant_b" } });
			await expect(calls.create(record)).rejects.toBeInstanceOf(PreconditionFailed);
			expect(await calls.listCalls(callId)).toEqual([scope, other]);
			expect(await calls.listCalls(`${callId}x`)).toEqual([]);
			expect((await calls.read(other))?.record.tenantId).toBe("tenant_b");
			for (const s of [scope, other]) {
				await calls.objects.delete(`calls/${callId}/${s.tenantId}`);
				await calls.objects.delete(`evidence/${callId}/${s.tenantId}`);
			}
			await calls.objects.delete(`cancel/${callId}/tenant_a`);
		});
	});
}

semantics("filesystem store (DEVELOPMENT_ONLY)", async () => new FilesystemStore(scratch()));

const s3env = {
	endpoint: process.env.ANVILKIT_DEV_ARTIFACTS_ENDPOINT,
	bucket: process.env.ANVILKIT_DEV_MODEL_PROXY_BUCKET,
	accessKeyId: process.env.ANVILKIT_DEV_MODEL_PROXY_ACCESS_KEY_ID,
	secretAccessKey: process.env.ANVILKIT_DEV_MODEL_PROXY_SECRET_ACCESS_KEY,
};
if (s3env.endpoint && s3env.bucket && s3env.accessKeyId && s3env.secretAccessKey) {
	semantics(
		"s3 store on the development foundation's MinIO",
		async () =>
			new S3Store({
				endpoint: s3env.endpoint as string,
				bucket: s3env.bucket as string,
				region: "default",
				prefix: `test-${process.pid}`,
				pathStyle: true,
				qualifyOnStart: true,
				accessKeyId: s3env.accessKeyId as string,
				secretAccessKey: s3env.secretAccessKey as string,
			}),
	);
} else {
	describe.skip("s3 store on the development foundation's MinIO (ANVILKIT_DEV_MODEL_PROXY_* not set; source .local/dev/env.sh)", () => {
		it("skipped", () => {});
	});
}
