import { existsSync, readFileSync } from "node:fs";
import { Agent } from "node:http";
import type { Socket } from "node:net";
import path from "node:path";
import { DispatchOutcome, DispatchState } from "@anvilkit/generated-clients/proto/anvilkit/control/v1/dispatch";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type { ObjectStore, PutOptions } from "../src/adapters/store.js";
import type { StreamFrame } from "../src/contracts.js";
import { digestOf, encodedFrameBytes, toolSchemaDigest } from "../src/domain/call.js";
import { FakeControl } from "./control.js";
import {
	call,
	controlToken,
	credential,
	digest,
	type RunningProxy,
	requestBody,
	sidecarToken,
	startProxy,
	token,
} from "./proxy.js";
import { Upstream } from "./upstream.js";

const readBriefSchema = {
	type: "object",
	additionalProperties: false,
	properties: { section: { type: "string", maxLength: 64 } },
};

describe("controlled calls", () => {
	let up: Upstream;
	let control: FakeControl;
	let proxy: RunningProxy;
	let n = 0;
	const id = (name: string) => `call_${name}_${++n}_${Date.now()}`;
	/** The scope of the scenarios' calls: requestBody binds them to tenant_a. */
	const scope = (callId: string) => ({ tenantId: "tenant_a", callId });

	beforeAll(async () => {
		up = await new Upstream().start();
		control = await new FakeControl().start();
		proxy = await startProxy({ upstreamUrl: up.url, controlAddress: control.address, instanceId: "a" });
	});
	afterEach(() => {
		up.release();
		control.loseAdmitAnswers = 0;
		control.loseObserveAnswers = 0;
		control.unavailable = 0;
	});
	afterAll(async () => {
		await proxy.close();
		await control.stop();
		await up.stop();
	});

	const evidenceOf = (callId: string) =>
		JSON.parse(readFileSync(path.join(proxy.storeDir, "evidence", callId, "tenant_a"), "utf8"));

	it("a normal call: one admission, one physical send, bounded frames, evidence and one observation with native usage", async () => {
		up.next({
			kind: "stream",
			text: ["Hello", " world"],
			usage: {
				prompt_tokens: 120,
				completion_tokens: 34,
				prompt_tokens_details: { cached_tokens: 20 },
				completion_tokens_details: { reasoning_tokens: 4 },
			},
		});
		const callId = id("normal");
		const sends = up.receives.length;
		const r = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(callId));
		expect(r.status, r.text).toBe(200);
		expect(r.headers["content-type"]).toMatch(/text\/event-stream/);
		expect(r.frames.map((f) => `${f.sequence}:${f.type}`)).toEqual([
			"0:admitted",
			"1:text",
			"2:text",
			"3:usage",
			"4:done",
		]);
		expect(r.frames.every((f) => f.callId === callId)).toBe(true);
		const usage = { inputUnits: "120", outputUnits: "34", reasoningUnits: "4", cachedInputUnits: "20" };
		expect(r.frames[4]).toMatchObject({ type: "done", outcome: "succeeded", usage });
		expect(r.text).not.toContain("chat.completion.chunk");
		expect(up.receives.length).toBe(sends + 1);
		expect(up.receives.at(-1)?.headers.authorization).toBe(`Bearer ${credential}`);
		expect(up.receives.at(-1)?.body).not.toContain(token);
		const dispatch = control.byCall(callId);
		expect(dispatch?.state).toBe(DispatchState.DISPATCH_STATE_OBSERVED);
		expect(dispatch?.observations).toHaveLength(1);
		expect(dispatch?.observations[0]).toMatchObject({
			source: "model-proxy/a",
			outcome: DispatchOutcome.DISPATCH_OUTCOME_SUCCEEDED,
			usage: { input: "120", output: "34", reasoning: "4", cached: "20" },
			nativeReference: "chatcmpl-fixture",
		});
		expect(control.admits.filter((a) => a.callId === callId)).toHaveLength(1);
		const admit = control.admits.find((a) => a.callId === callId);
		expect(admit?.command?.actorId).toBe("anvilkit-agent-workflow");
		expect(admit?.owner).toBe("anvilkit-agent-model-proxy");
		expect(admit).toMatchObject({ provider: "fixture", model: "fixture-model", routeId: "controlled-openai-v1" });
		const g = await call(proxy.url, "GET", `/api/v1/model-calls/${callId}`);
		expect(g.status).toBe(200);
		expect(g.json).toMatchObject({
			callId,
			routeId: "controlled-openai-v1",
			state: "succeeded",
			dispatchId: dispatch?.dispatchId,
			usage,
			nativeReference: "chatcmpl-fixture",
		});
		expect(g.text).not.toContain("frames");
		const ev = evidenceOf(callId);
		expect(ev.request.url).toBe(`${up.url}/v1/chat/completions`);
		expect(ev.request.body).toContain("Plan the hero component.");
		expect(ev.request.body).not.toContain(credential);
		expect(Buffer.from(ev.response.bodyBase64, "base64").toString()).toContain('"prompt_tokens":120');
		expect(existsSync(path.join(proxy.storeDir, "pending", callId, "tenant_a"))).toBe(false);
	});

	it("tool calls carry the arguments digest and must name a reviewed schema by its digest", async () => {
		up.next({
			kind: "stream",
			text: ["Reading"],
			toolCall: { id: "tc_1", name: "read_brief", arguments: '{"section":"hero"}' },
		});
		const callId = id("tool");
		const ok = await call(
			proxy.url,
			"POST",
			"/api/v1/model-calls",
			requestBody(callId, { tools: [{ name: "read_brief", inputSchemaDigest: toolSchemaDigest(readBriefSchema) }] }),
		);
		expect(ok.status, ok.text).toBe(200);
		const tc = ok.frames.find((f) => f.type === "tool_call");
		expect(tc?.toolCall).toEqual({
			toolCallId: "tc_1",
			name: "read_brief",
			argumentsDigest: digestOf('{"section":"hero"}'),
			arguments: '{"section":"hero"}',
		});
		const body = JSON.parse(up.receives.at(-1)?.body ?? "{}") as { tools: { function: { parameters: unknown } }[] };
		expect(body.tools[0]?.function.parameters).toEqual(readBriefSchema);
		const sends = up.receives.length;
		const wrongDigest = await call(
			proxy.url,
			"POST",
			"/api/v1/model-calls",
			requestBody(id("tool-bad"), { tools: [{ name: "read_brief", inputSchemaDigest: digestOf("other schema") }] }),
		);
		expect(wrongDigest.status).toBe(403);
		expect((wrongDigest.json as { error: { code: string } }).error.code).toBe("PROFILE_UNQUALIFIED");
		const unknownTool = await call(
			proxy.url,
			"POST",
			"/api/v1/model-calls",
			requestBody(id("tool-unknown"), { tools: [{ name: "delete_all", inputSchemaDigest: digestOf("x") }] }),
		);
		expect(unknownTool.status).toBe(403);
		expect(up.receives.length).toBe(sends);
	});

	it("a tool round trip: the tool call comes back, the caller executes it, the next call replays the assistant tool call and the matching result under its own admission", async () => {
		const tools = [{ name: "read_brief", inputSchemaDigest: toolSchemaDigest(readBriefSchema) }];
		up.next({
			kind: "stream",
			text: ["Reading"],
			toolCall: { id: "tc_rt", name: "read_brief", arguments: '{"section":"hero"}' },
		});
		const first = id("rt-first");
		const r1 = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(first, { tools }));
		expect(r1.status, r1.text).toBe(200);
		const tc = r1.frames.find((f) => f.type === "tool_call")?.toolCall;
		if (!tc) throw new Error("no tool call frame");
		expect(tc.arguments).toBe('{"section":"hero"}');
		// The caller executes the tool (the Proxy never does) and asks again with the round trip in the history.
		const history = [
			{ role: "system" as const, content: "You are the planner." },
			{ role: "user" as const, content: "Plan the hero component." },
			{
				role: "assistant" as const,
				content: "Reading",
				toolCalls: [{ toolCallId: tc.toolCallId, name: tc.name, arguments: tc.arguments ?? "" }],
			},
			{ role: "tool" as const, toolCallId: tc.toolCallId, content: "The brief: one hero, one call to action." },
		];
		up.next({ kind: "stream", text: ["One hero it is."] });
		const second = id("rt-second");
		const admits = control.admits.length;
		const sends = up.receives.length;
		const r2 = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(second, { tools, messages: history }));
		expect(r2.status, r2.text).toBe(200);
		expect(r2.frames.at(-1)?.type).toBe("done");
		expect(control.admits.length).toBe(admits + 1);
		expect(control.admits.at(-1)?.callId).toBe(second);
		expect(control.byCall(second)?.dispatchId).not.toBe(control.byCall(first)?.dispatchId);
		expect(up.receives.length).toBe(sends + 1);
		const sent = JSON.parse(up.receives.at(-1)?.body ?? "{}") as { messages: Record<string, unknown>[] };
		expect(sent.messages).toHaveLength(4);
		expect(sent.messages[2]).toMatchObject({
			role: "assistant",
			content: "Reading",
			tool_calls: [
				{ id: tc.toolCallId, type: "function", function: { name: "read_brief", arguments: '{"section":"hero"}' } },
			],
		});
		expect(sent.messages[3]).toEqual({
			role: "tool",
			tool_call_id: tc.toolCallId,
			content: "The brief: one hero, one call to action.",
		});
		expect(JSON.stringify(sent.messages)).not.toContain("toolCalls");
		// A history outside the round trip is refused before any admission.
		const before = control.admits.length;
		for (const [name, messages, code] of [
			[
				"a tool result answering no earlier call",
				[history[0], history[1], { role: "tool" as const, toolCallId: "tc_none", content: "x" }],
				"INVALID_ARGUMENT",
			],
			[
				"tool calls on a user message",
				[history[0], { role: "user" as const, content: "x", toolCalls: history[2]?.toolCalls }],
				"INVALID_ARGUMENT",
			],
			[
				"a tool result without its call id",
				[history[0], history[1], history[2], { role: "tool" as const, content: "x" }],
				"INVALID_ARGUMENT",
			],
			["a tool call answered twice", [...history, history[3]], "INVALID_ARGUMENT"],
			[
				"arguments that are not a JSON object",
				[
					history[0],
					history[1],
					{ ...history[2], toolCalls: [{ toolCallId: "tc_a", name: "read_brief", arguments: "[1]" }] },
				],
				"INVALID_ARGUMENT",
			],
			[
				"a tool call of an unreviewed tool",
				[
					history[0],
					history[1],
					{ ...history[2], toolCalls: [{ toolCallId: "tc_b", name: "delete_all", arguments: "{}" }] },
				],
				"PROFILE_UNQUALIFIED",
			],
		] as const) {
			const r = await call(
				proxy.url,
				"POST",
				"/api/v1/model-calls",
				requestBody(id("rt-bad"), { tools, messages: messages as never }),
			);
			expect(r.status, name).toBeGreaterThanOrEqual(400);
			expect((r.json as { error: { code: string } }).error.code, name).toBe(code);
		}
		expect(control.admits.length).toBe(before);
		expect(up.receives.length).toBe(sends + 1);
	});

	it("a reentry replays the recorded frames without a second admission or send; changed content conflicts", async () => {
		up.next({ kind: "stream", text: ["Once"] });
		const callId = id("reentry");
		const body = requestBody(callId);
		const first = await call(proxy.url, "POST", "/api/v1/model-calls", body);
		expect(first.status).toBe(200);
		const sends = up.receives.length;
		const admits = control.admits.length;
		const again = await call(proxy.url, "POST", "/api/v1/model-calls", body);
		expect(again.status).toBe(200);
		expect(again.frames).toEqual(first.frames);
		expect(up.receives.length).toBe(sends);
		expect(control.admits.length).toBe(admits);
		const changed = await call(
			proxy.url,
			"POST",
			"/api/v1/model-calls",
			body.replace("Plan the hero component.", "Something else."),
		);
		expect(changed.status).toBe(409);
		const laterDeadline = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(callId));
		expect(laterDeadline.status, "a reentry must carry the original deadline").toBe(409);
		expect(changed.status).toBe(409);
		expect((changed.json as { error: { code: string } }).error.code).toBe("IDEMPOTENCY_CONFLICT");
		const otherDigest = await call(proxy.url, "POST", "/api/v1/model-calls", body.replace(digest, digestOf("other")));
		expect(otherDigest.status).toBe(409);
		// The same call id under another tenant is not this call: its own
		// admission, its own send, its own record — nothing of this one.
		up.next({ kind: "stream", text: ["Twice"] });
		const otherTenant = await call(
			proxy.url,
			"POST",
			"/api/v1/model-calls",
			body.replace('"tenantId":"tenant_a"', '"tenantId":"tenant_b"'),
		);
		expect(otherTenant.status, otherTenant.text).toBe(200);
		expect(otherTenant.frames.find((f) => f.type === "text")?.text).toBe("Twice");
		expect(up.receives.length).toBe(sends + 1);
		expect(control.admits.length).toBe(admits + 1);
		expect(control.dispatches.get(`tenant_b/anvilkit-agent-model-proxy/${callId}`)?.observations).toHaveLength(1);
		expect((await call(proxy.url, "POST", "/api/v1/model-calls", body)).frames).toEqual(first.frames);
		expect(up.receives.length).toBe(sends + 1);
	});

	it("a denied admission sends nothing, is recorded and answers the same denial again", async () => {
		control.denials.set("controlled-openai-v1", "BUDGET_EXHAUSTED");
		try {
			const callId = id("denied");
			const body = requestBody(callId);
			const sends = up.receives.length;
			const r = await call(proxy.url, "POST", "/api/v1/model-calls", body);
			expect(r.status).toBe(429);
			expect((r.json as { error: { code: string; retryable: boolean } }).error).toMatchObject({
				code: "BUDGET_EXHAUSTED",
				retryable: false,
			});
			const again = await call(proxy.url, "POST", "/api/v1/model-calls", body);
			expect(again.status).toBe(429);
			expect(up.receives.length).toBe(sends);
			const g = await call(proxy.url, "GET", `/api/v1/model-calls/${callId}`);
			expect(g.json).toMatchObject({ state: "denied", errorCode: "BUDGET_EXHAUSTED" });
			expect(control.observes.filter((o) => o.dispatchId === control.byCall(callId)?.dispatchId)).toHaveLength(0);
		} finally {
			control.denials.delete("controlled-openai-v1");
		}
	});

	it("a lost first permission answer causes no send: the call is unknown and Control retains the exposure", async () => {
		control.loseAdmitAnswers = 1;
		const callId = id("lost");
		const body = requestBody(callId);
		const sends = up.receives.length;
		const r = await call(proxy.url, "POST", "/api/v1/model-calls", body);
		expect(r.status, r.text).toBe(200);
		expect(r.frames.map((f) => f.type)).toEqual(["admitted", "error"]);
		expect(r.frames[1]).toMatchObject({ outcome: "unknown", errorCode: "PERMISSION_LOST" });
		expect(up.receives.length).toBe(sends);
		expect(control.admits.filter((a) => a.callId === callId).length).toBeGreaterThanOrEqual(2);
		expect(control.byCall(callId)?.state).toBe(DispatchState.DISPATCH_STATE_UNKNOWN);
		const g = await call(proxy.url, "GET", `/api/v1/model-calls/${callId}`);
		expect(g.json).toMatchObject({ state: "unknown", errorCode: "PERMISSION_LOST" });
		const again = await call(proxy.url, "POST", "/api/v1/model-calls", body);
		expect(again.frames).toEqual(r.frames);
		expect(up.receives.length).toBe(sends);
	});

	it("an unreachable Control is a retryable refusal with nothing sent and nothing recorded", async () => {
		control.unavailable = 10;
		const callId = id("unavailable");
		const body = requestBody(callId);
		const sends = up.receives.length;
		const r = await call(proxy.url, "POST", "/api/v1/model-calls", body);
		expect(r.status).toBe(503);
		expect((r.json as { error: { code: string; retryable: boolean } }).error).toMatchObject({
			code: "DEPENDENCY_UNAVAILABLE",
			retryable: true,
		});
		expect(up.receives.length).toBe(sends);
		expect(existsSync(path.join(proxy.storeDir, "calls", callId))).toBe(false);
		control.unavailable = 0;
		up.next({ kind: "stream", text: ["later"] });
		const later = await call(proxy.url, "POST", "/api/v1/model-calls", body);
		expect(later.status).toBe(200);
		expect(later.frames.at(-1)?.type).toBe("done");
		expect(up.receives.length).toBe(sends + 1);
	});

	it("upstream failures reach the upstream once and end as unknown outcomes that retain the exposure", async () => {
		for (const [name, behavior, code] of [
			["a 500", { kind: "status" as const, status: 500 }, "UPSTREAM_STATUS_500"],
			["a 429", { kind: "status" as const, status: 429 }, "UPSTREAM_STATUS_429"],
			[
				"a redirect",
				{ kind: "redirect" as const, location: `${up.targetUrl}/v1/chat/completions` },
				"UPSTREAM_STATUS_307",
			],
			["a dropped connection", { kind: "close" as const }, "UPSTREAM_ERROR"],
		] as const) {
			up.next(behavior);
			const callId = id("fail");
			const sends = up.receives.length;
			const r = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(callId));
			expect(r.status, name).toBe(200);
			expect(r.frames.at(-1), name).toMatchObject({ type: "error", outcome: "unknown", errorCode: code });
			expect(r.frames.at(-1)?.usage, name).toBeUndefined();
			expect(up.receives.length, name).toBe(sends + 1);
			expect(up.redirectTargetReceives.length, name).toBe(0);
			expect(control.byCall(callId)?.state, name).toBe(DispatchState.DISPATCH_STATE_UNKNOWN);
			expect(control.byCall(callId)?.observations[0]?.usage, name).toBeUndefined();
		}
	});

	it("a refused connection is a certain not-sent: failed with explicit zero usage", async () => {
		const closed = await new Upstream().start();
		const url = closed.url;
		await closed.closeListener();
		const other = await startProxy({ upstreamUrl: url, controlAddress: control.address, instanceId: "closed" });
		try {
			const callId = id("refused");
			const r = await call(other.url, "POST", "/api/v1/model-calls", requestBody(callId));
			expect(r.status).toBe(200);
			expect(r.frames.at(-1)).toMatchObject({
				type: "error",
				outcome: "failed",
				errorCode: "UPSTREAM_UNREACHABLE",
				usage: { inputUnits: "0", outputUnits: "0" },
			});
			expect(control.byCall(callId)?.state).toBe(DispatchState.DISPATCH_STATE_OBSERVED);
		} finally {
			await other.close();
		}
	});

	it("missing usage stays distinct from explicit zero usage", async () => {
		up.next({ kind: "stream", text: ["no usage chunk"], usage: null });
		const missing = id("missing-usage");
		const r = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(missing));
		expect(r.frames.map((f) => f.type)).toEqual(["admitted", "text", "error"]);
		expect(r.frames.at(-1)).toMatchObject({ outcome: "unknown", errorCode: "USAGE_MISSING" });
		expect(control.byCall(missing)?.state).toBe(DispatchState.DISPATCH_STATE_UNKNOWN);
		expect(
			control.observes.find((o) => o.dispatchId === control.byCall(missing)?.dispatchId)?.cumulativeUsage,
		).toBeUndefined();
		up.next({ kind: "stream", text: ["free"], usage: { prompt_tokens: 0, completion_tokens: 0 } });
		const zero = id("zero-usage");
		const z = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(zero));
		expect(z.frames.at(-1)).toMatchObject({
			type: "done",
			usage: { inputUnits: "0", outputUnits: "0", reasoningUnits: "0", cachedInputUnits: "0" },
		});
		expect(control.byCall(zero)?.state).toBe(DispatchState.DISPATCH_STATE_OBSERVED);
		expect(
			control.observes.find((o) => o.dispatchId === control.byCall(zero)?.dispatchId)?.cumulativeUsage,
		).toMatchObject({ inputUnits: "0" });
		// Optional categories absent are zero; present, they are validated as subsets.
		up.next({
			kind: "stream",
			text: ["partial details"],
			usage: {
				prompt_tokens: 40,
				completion_tokens: 10,
				total_tokens: 50,
				prompt_tokens_details: { cached_tokens: 8 },
			},
		});
		const partial = id("partial-details");
		const pr = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(partial));
		expect(pr.frames.at(-1)).toMatchObject({
			type: "done",
			usage: { inputUnits: "40", outputUnits: "10", reasoningUnits: "0", cachedInputUnits: "8" },
		});
	});

	it("usage outside the protocol never settles: an empty object, missing, negative, fractional, typed-wrong or inconsistent counters end unknown with the exposure retained; later observations stay idempotent", async () => {
		for (const [name, usage] of [
			["an empty usage object", {}],
			["a missing completion counter", { prompt_tokens: 5 }],
			["a missing prompt counter", { completion_tokens: 5 }],
			["a negative counter", { prompt_tokens: -1, completion_tokens: 2 }],
			["a fractional counter", { prompt_tokens: 1.5, completion_tokens: 2 }],
			["a string counter", { prompt_tokens: "3", completion_tokens: 2 }],
			["a null counter", { prompt_tokens: null, completion_tokens: 2 }],
			["an inconsistent total", { prompt_tokens: 3, completion_tokens: 2, total_tokens: 9 }],
			[
				"a cached count above the prompt",
				{ prompt_tokens: 3, completion_tokens: 2, prompt_tokens_details: { cached_tokens: 4 } },
			],
			[
				"a reasoning count above the completion",
				{ prompt_tokens: 3, completion_tokens: 2, completion_tokens_details: { reasoning_tokens: 3 } },
			],
			["a detail that is not an object", { prompt_tokens: 3, completion_tokens: 2, prompt_tokens_details: 7 }],
			["a usage that is an array", [3, 2]],
		] as const) {
			up.next({ kind: "stream", text: ["counted?"], usage });
			const callId = id("invalid-usage");
			const sends = up.receives.length;
			const body = requestBody(callId);
			const r = await call(proxy.url, "POST", "/api/v1/model-calls", body);
			expect(r.status, name).toBe(200);
			expect(
				r.frames.map((f) => f.type),
				name,
			).toEqual(["admitted", "text", "error"]);
			expect(r.frames.at(-1), name).toMatchObject({ outcome: "unknown", errorCode: "USAGE_INVALID" });
			expect(r.frames.at(-1)?.usage, name).toBeUndefined();
			expect(up.receives.length, name).toBe(sends + 1);
			const dispatch = control.byCall(callId);
			expect(dispatch?.state, name).toBe(DispatchState.DISPATCH_STATE_UNKNOWN);
			expect(dispatch?.observations, name).toHaveLength(1);
			expect(dispatch?.observations[0]?.usage, name).toBeUndefined();
			const g = await call(proxy.url, "GET", `/api/v1/model-calls/${callId}`);
			expect(g.json, name).toMatchObject({ state: "unknown", errorCode: "USAGE_INVALID" });
			expect((g.json as { usage?: unknown }).usage, name).toBeUndefined();
			// The evidence keeps what the provider reported, for reconciliation.
			expect(Buffer.from(evidenceOf(callId).response.bodyBase64, "base64").toString(), name).toContain('"usage":');
			// A reentry replays the same outcome and adds no observation; the sweep adds none either.
			const again = await call(proxy.url, "POST", "/api/v1/model-calls", body);
			expect(again.frames, name).toEqual(r.frames);
			await proxy.calls.sweep();
			expect(control.byCall(callId)?.observations, name).toHaveLength(1);
			expect(up.receives.length, name).toBe(sends + 1);
		}
	});

	it("cancellation before the send sends nothing; after the send it aborts and keeps the outcome unknown without usage", async () => {
		const before = id("cancel-before");
		await proxy.store.requestCancel(scope(before), new Date().toISOString());
		const sends = up.receives.length;
		const r = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(before));
		expect(r.status).toBe(200);
		expect(r.frames.at(-1)).toMatchObject({
			type: "error",
			outcome: "canceled",
			errorCode: "CANCELED_BEFORE_SEND",
			usage: { inputUnits: "0" },
		});
		expect(up.receives.length).toBe(sends);
		expect(control.byCall(before)?.observations[0]).toMatchObject({
			outcome: DispatchOutcome.DISPATCH_OUTCOME_CANCELED,
			usage: { input: "0", output: "0" },
		});

		up.next({ kind: "stream-then-hang", text: ["partial"] });
		const after = id("cancel-after");
		const streaming = call(proxy.url, "POST", "/api/v1/model-calls", requestBody(after));
		await up.awaitHeld();
		const c = await call(proxy.url, "POST", `/api/v1/model-calls/${after}/cancellations`, "{}");
		expect(c.status).toBe(202);
		expect((c.json as { callId: string }).callId).toBe(after);
		const s = await streaming;
		expect(s.frames.map((f) => f.type)).toEqual(["admitted", "text", "error"]);
		expect(s.frames.at(-1)).toMatchObject({ outcome: "unknown", errorCode: "CANCELED" });
		expect(up.receives.length).toBe(sends + 1);
		expect(control.byCall(after)?.state).toBe(DispatchState.DISPATCH_STATE_UNKNOWN);
		const g = await call(proxy.url, "GET", `/api/v1/model-calls/${after}`);
		expect(g.json).toMatchObject({ state: "unknown", errorCode: "CANCELED" });
		const missing = await call(proxy.url, "POST", "/api/v1/model-calls/call_never/cancellations", "{}");
		expect(missing.status).toBe(404);
	});

	it("the original deadline bounds the send; the route bounds the output", async () => {
		up.next({ kind: "stream-then-hang", text: ["slow"] });
		const late = id("deadline");
		const t0 = Date.now();
		const r = await call(
			proxy.url,
			"POST",
			"/api/v1/model-calls",
			requestBody(late, { deadline: new Date(Date.now() + 1500).toISOString() }),
		);
		expect(r.frames.at(-1)).toMatchObject({ type: "error", outcome: "unknown", errorCode: "DEADLINE_EXCEEDED" });
		expect(Date.now() - t0).toBeLessThan(6_000);
		const past = await call(
			proxy.url,
			"POST",
			"/api/v1/model-calls",
			requestBody(id("past"), { deadline: new Date(Date.now() - 1000).toISOString() }),
		);
		expect(past.status).toBe(403);
		expect((past.json as { error: { code: string } }).error.code).toBe("STALE_EXECUTION");
		const far = await call(
			proxy.url,
			"POST",
			"/api/v1/model-calls",
			requestBody(id("far"), { deadline: new Date(Date.now() + 3_600_000).toISOString() }),
		);
		expect(far.status).toBe(400);
		const tooMany = await call(
			proxy.url,
			"POST",
			"/api/v1/model-calls",
			requestBody(id("tokens"), { maxOutputTokens: 5000 }),
		);
		expect(tooMany.status).toBe(400);
		const exposure = await call(
			proxy.url,
			"POST",
			"/api/v1/model-calls",
			requestBody(id("exposure"), { maxExposure: { currency: "USD", amount: "1000001" } }),
		);
		expect(exposure.status).toBe(400);
		const currency = await call(
			proxy.url,
			"POST",
			"/api/v1/model-calls",
			requestBody(id("currency"), { maxExposure: { currency: "EUR", amount: "1" } }),
		);
		expect(currency.status).toBe(400);
		const bounded = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "bounded",
			configEdits: (y) => y.replace("max_output_bytes: 1048576", "max_output_bytes: 8"),
		});
		try {
			up.next({ kind: "stream", text: ["twelve chars", " more"] });
			const b = await call(bounded.url, "POST", "/api/v1/model-calls", requestBody(id("bound")));
			expect(b.frames.at(-1)?.type).toBe("error");
			expect(b.frames.at(-1)?.errorCode).toBe("OUTPUT_BOUND_EXCEEDED");
			// The output bound counts UTF-8 bytes: four three-byte characters exceed 8 bytes.
			up.next({ kind: "stream", text: ["日本語文"] });
			const bytes = await call(bounded.url, "POST", "/api/v1/model-calls", requestBody(id("bound-bytes")));
			expect(bytes.frames.at(-1)?.errorCode).toBe("OUTPUT_BOUND_EXCEEDED");
			up.next({ kind: "stream", text: ["日本"] });
			const fits = await call(bounded.url, "POST", "/api/v1/model-calls", requestBody(id("bound-fits")));
			expect(fits.frames.at(-1)?.type).toBe("done");
		} finally {
			await bounded.close();
		}
	});

	it("frame bounds are UTF-8 byte bounds on the encoded frame with the final frame's slot reserved: text splits without breaking a code point, arguments and frame counts have explicit outcomes, client and record agree", async () => {
		const framed = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "framed",
			configEdits: (y) =>
				y.replace("max_frame_bytes: 65536", "max_frame_bytes: 1024").replace("max_frames: 4096", "max_frames: 6"),
		});
		try {
			// 300 four-byte characters (1200 bytes) arrive in fragments that cut
			// multibyte sequences; the frames carry them whole, within the bound.
			const text = "🎉".repeat(300);
			up.next({ kind: "stream", text: [text], fragmentBytes: 7 });
			const callId = id("frame-bytes");
			const r = await call(framed.url, "POST", "/api/v1/model-calls", requestBody(callId));
			expect(r.status, r.text).toBe(200);
			const texts = r.frames.filter((f) => f.type === "text");
			expect(texts.length).toBeGreaterThan(1);
			for (const f of r.frames) expect(encodedFrameBytes(f)).toBeLessThanOrEqual(1024);
			expect(texts.map((f) => f.text).join("")).toBe(text);
			for (const f of texts) expect((f.text ?? "").length % 2, "no broken surrogate pair").toBe(0);
			expect(r.frames.at(-1)).toMatchObject({ type: "done", outcome: "succeeded" });
			const rec = await framed.store.read(scope(callId));
			expect(rec?.record.frames).toEqual(r.frames);
			// Six frames at most: admitted, four content frames, the final one. Content
			// needing the last slot ends the call with the bound as its explicit outcome —
			// the final frame is never lost to the count.
			up.next({ kind: "stream", text: ["a", "b", "c", "d", "e", "f"] });
			const many = id("frame-count");
			const m = await call(framed.url, "POST", "/api/v1/model-calls", requestBody(many));
			expect(m.frames.map((f) => f.type)).toEqual(["admitted", "text", "text", "text", "text", "error"]);
			// The stream was cut at the bound; the usage chunk of the double's one write was
			// already captured, so the outcome is failed with the usage (unknown without it).
			expect(m.frames.at(-1)).toMatchObject({ errorCode: "SEQUENCE_BOUND_EXCEEDED" });
			expect(["failed", "unknown"]).toContain(m.frames.at(-1)?.outcome);
			if (m.frames.at(-1)?.outcome === "failed") expect(m.frames.at(-1)?.usage).toMatchObject({ inputUnits: "12" });
			const manyRecord = (await framed.store.read(scope(many)))?.record;
			expect(manyRecord).toMatchObject({ state: m.frames.at(-1)?.outcome, errorCode: "SEQUENCE_BOUND_EXCEEDED" });
			expect(manyRecord?.frames).toEqual(m.frames);
			// Exactly four content frames plus the usage frame do not fit: the usage frame is
			// informational and yields; the done frame carries the usage.
			up.next({ kind: "stream", text: ["a", "b", "c", "d"] });
			const exact = id("frame-exact");
			const e = await call(framed.url, "POST", "/api/v1/model-calls", requestBody(exact));
			expect(e.frames.map((f) => f.type)).toEqual(["admitted", "text", "text", "text", "text", "done"]);
			expect(e.frames.at(-1)?.usage).toMatchObject({ inputUnits: "12" });
			// Tool arguments over the encoded frame bound: an explicit outcome, one send.
			const tools = [{ name: "read_brief", inputSchemaDigest: toolSchemaDigest(readBriefSchema) }];
			up.next({
				kind: "stream",
				text: [],
				toolCall: { id: "tc_big", name: "read_brief", arguments: JSON.stringify({ section: "x".repeat(1100) }) },
			});
			const big = id("frame-args");
			const sends = up.receives.length;
			const a = await call(framed.url, "POST", "/api/v1/model-calls", requestBody(big, { tools }));
			expect(a.frames.map((f) => f.type)).toEqual(["admitted", "error"]);
			expect(a.frames.at(-1)).toMatchObject({ errorCode: "FRAME_BOUND_EXCEEDED" });
			expect(["failed", "unknown"]).toContain(a.frames.at(-1)?.outcome);
			expect((await framed.store.read(scope(big)))?.record).toMatchObject({
				state: a.frames.at(-1)?.outcome,
				errorCode: "FRAME_BOUND_EXCEEDED",
			});
			expect(up.receives.length).toBe(sends + 1);
		} finally {
			await framed.close();
		}
	});

	it("a client disconnect does not erase incurred usage: the send completes, is observed and replays", async () => {
		up.next({
			kind: "stream",
			text: ["first", " second", " third"],
			usage: { prompt_tokens: 7, completion_tokens: 3 },
		});
		const callId = id("disconnect");
		const body = requestBody(callId);
		const partial = await call(proxy.url, "POST", "/api/v1/model-calls", body, token, (frames) => frames.length >= 2);
		expect(partial.frames.length).toBe(2);
		let g: Awaited<ReturnType<typeof call>>;
		for (let i = 0; i < 50; i++) {
			g = await call(proxy.url, "GET", `/api/v1/model-calls/${callId}`);
			if ((g.json as { state: string }).state === "succeeded") break;
			await new Promise((r) => setTimeout(r, 50));
		}
		// biome-ignore lint/style/noNonNullAssertion: assigned in the loop
		expect(g!.json).toMatchObject({ state: "succeeded", usage: { inputUnits: "7", outputUnits: "3" } });
		expect(control.byCall(callId)?.state).toBe(DispatchState.DISPATCH_STATE_OBSERVED);
		const replay = await call(proxy.url, "POST", "/api/v1/model-calls", body);
		expect(replay.frames.map((f) => f.type)).toEqual(["admitted", "text", "text", "text", "usage", "done"]);
	});

	it("one keep-alive connection serving completed streams, a mid-stream disconnect and more streams keeps no listener of the earlier responses: the counts on the socket stay at their baseline and Node raises no listener warning", async () => {
		const warnings: string[] = [];
		const onWarning = (w: Error) => warnings.push(w.name);
		process.on("warning", onWarning);
		const sockets: Socket[] = [];
		const onConnection = (s: Socket) => sockets.push(s);
		proxy.server.on("connection", onConnection);
		const agent = new Agent({ keepAlive: true, maxSockets: 1 });
		const countOn = (s: Socket) =>
			["close", "drain", "error", "end", "timeout", "finish"].map((e) => s.listenerCount(e));
		const settled = () => new Promise((r) => setTimeout(r, 30));
		try {
			let baseline: number[] | undefined;
			for (let i = 0; i < 14; i++) {
				up.next({ kind: "stream", text: ["k"], usage: { prompt_tokens: 1, completion_tokens: 1 } });
				const r = await call(
					proxy.url,
					"POST",
					"/api/v1/model-calls",
					requestBody(id("keepalive")),
					token,
					undefined,
					agent,
				);
				expect(r.status, r.text).toBe(200);
				expect(r.frames.at(-1)?.type).toBe("done");
				await settled();
				const socket = sockets.at(-1) as Socket;
				if (baseline === undefined) baseline = countOn(socket);
				else expect(countOn(socket), `after ${i + 1} streams`).toEqual(baseline);
			}
			expect(sockets, "every stream went over the one kept-alive connection").toHaveLength(1);
			// A caller that goes away mid-stream ends that connection; the ones after it start clean.
			up.next({
				kind: "stream",
				text: ["first", " second", " third"],
				usage: { prompt_tokens: 1, completion_tokens: 1 },
			});
			const gone = id("keepalive-gone");
			const partial = await call(
				proxy.url,
				"POST",
				"/api/v1/model-calls",
				requestBody(gone),
				token,
				(f) => f.length >= 2,
				agent,
			);
			expect(partial.frames).toHaveLength(2);
			for (let i = 0; i < 50 && (await proxy.store.read(scope(gone)))?.record.state !== "succeeded"; i++)
				await settled();
			expect((await proxy.store.read(scope(gone)))?.record.state).toBe("succeeded");
			for (let i = 0; i < 3; i++) {
				up.next({ kind: "stream", text: ["k"], usage: { prompt_tokens: 1, completion_tokens: 1 } });
				const r = await call(
					proxy.url,
					"POST",
					"/api/v1/model-calls",
					requestBody(id("keepalive-after")),
					token,
					undefined,
					agent,
				);
				expect(r.frames.at(-1)?.type).toBe("done");
				await settled();
				expect(countOn(sockets.at(-1) as Socket), `after the disconnect, stream ${i + 1}`).toEqual(baseline);
			}
			expect(sockets.length).toBe(2);
			await new Promise((r) => setImmediate(r));
			expect(warnings).not.toContain("MaxListenersExceededWarning");
		} finally {
			agent.destroy();
			proxy.server.off("connection", onConnection);
			process.off("warning", onWarning);
		}
	});

	it("a cancel that lands between the settlement's read and its conditional write does not lose the outcome: the record is reapplied with the usage, the caller receives the persisted outcome, and the marker precedes the record", async () => {
		let raced: RunningProxy | undefined;
		let armed = false;
		let interleaved = false;
		let markerBeforeRecord: boolean | undefined;
		const wrap = (inner: ObjectStore): ObjectStore => ({
			...inner,
			get: (k) => inner.get(k),
			delete: (k) => inner.delete(k),
			list: (p) => inner.list(p),
			qualify: () => inner.qualify(),
			describe: () => inner.describe(),
			put: async (key: string, body: Uint8Array, opts?: PutOptions) => {
				if (key.startsWith("calls/") && opts?.ifNoneMatch) {
					markerBeforeRecord = existsSync(
						path.join(inner.describe().split(" ")[1] ?? "", "pending", key.slice("calls/".length)),
					);
				}
				if (armed && key.startsWith("calls/") && opts?.ifMatch) {
					const record = JSON.parse(new TextDecoder().decode(body)) as { state: string; callId: string };
					if (record.state === "succeeded" && raced && !interleaved) {
						// The cancel moves the record after the settlement read it and before it writes.
						interleaved = true;
						await raced.calls.cancel({ id: "anvilkit-agent-workflow", kind: "workflow" }, record.callId);
					}
				}
				return inner.put(key, body, opts);
			},
		});
		raced = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "raced",
			objects: wrap,
		});
		try {
			armed = true;
			up.next({ kind: "stream", text: ["settled"], usage: { prompt_tokens: 8, completion_tokens: 2 } });
			const callId = id("cas-race");
			const sends = up.receives.length;
			const r = await call(raced.url, "POST", "/api/v1/model-calls", requestBody(callId));
			expect(interleaved).toBe(true);
			expect(markerBeforeRecord).toBe(true);
			expect(r.frames.at(-1)).toMatchObject({
				type: "done",
				outcome: "succeeded",
				usage: { inputUnits: "8", outputUnits: "2" },
			});
			const rec = (await raced.store.read(scope(callId)))?.record;
			expect(rec).toMatchObject({ state: "succeeded", usage: { inputUnits: "8", outputUnits: "2" } });
			expect(rec?.cancelRequestedAt).toBeDefined();
			expect(rec?.observations).toHaveLength(1);
			expect(rec?.observations[0]?.submitted).toBe(true);
			expect(control.byCall(callId)?.observations).toHaveLength(1);
			expect(control.byCall(callId)?.observations[0]).toMatchObject({ usage: { input: "8", output: "2" } });
			expect(existsSync(path.join(raced.storeDir, "pending", callId, "tenant_a"))).toBe(false);
			expect(up.receives.length).toBe(sends + 1);
		} finally {
			await raced.close();
		}
	});

	it("the permission holder takes over the PERMISSION_LOST placeholder a duplicate recorded under its identity and sends once; a placeholder of another dispatch or another caller is never overwritten", async () => {
		// The placeholder appears between the holder's read and its create: the
		// store double writes it from the holder's own record so the identity
		// (caller, tenant, digests) is exact, varying only what each case tests.
		let placeholder: ((holder: Record<string, unknown>) => Record<string, unknown>) | undefined;
		let created: string[] = [];
		const wrap = (inner: ObjectStore): ObjectStore => ({
			...inner,
			get: (k) => inner.get(k),
			delete: (k) => inner.delete(k),
			list: (p) => inner.list(p),
			qualify: () => inner.qualify(),
			describe: () => inner.describe(),
			put: async (key: string, body: Uint8Array, opts?: PutOptions) => {
				if (key.startsWith("calls/") && opts?.ifNoneMatch && placeholder) {
					const holder = JSON.parse(new TextDecoder().decode(body)) as Record<string, unknown>;
					if (holder.state === "sending") {
						const stale = placeholder;
						placeholder = undefined;
						await inner.put(key, new TextEncoder().encode(JSON.stringify(stale(holder))), { ifNoneMatch: true });
					}
				}
				if (key.startsWith("calls/") && opts?.ifMatch) {
					created.push((JSON.parse(new TextDecoder().decode(body)) as { state: string }).state);
				}
				return inner.put(key, body, opts);
			},
		});
		const holder = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "h",
			objects: wrap,
		});
		try {
			// The duplicate's placeholder under the same identity and dispatch: taken over, one send.
			placeholder = (h) => ({
				...h,
				state: "unknown",
				errorCode: "PERMISSION_LOST",
				frames: [{ callId: h.callId, sequence: "0", type: "admitted" }],
			});
			up.next({ kind: "stream", text: ["taken over"], usage: { prompt_tokens: 3, completion_tokens: 1 } });
			const taken = id("takeover");
			let sends = up.receives.length;
			const r = await call(holder.url, "POST", "/api/v1/model-calls", requestBody(taken));
			expect(r.status, r.text).toBe(200);
			expect(r.frames.map((f) => f.type)).toEqual(["admitted", "text", "usage", "done"]);
			expect(created[0]).toBe("sending");
			expect(up.receives.length).toBe(sends + 1);
			const rec = (await holder.store.read(scope(taken)))?.record;
			expect(rec).toMatchObject({ state: "succeeded", dispatchId: control.byCall(taken)?.dispatchId });
			expect(control.byCall(taken)?.observations).toHaveLength(1);
			// A record of another dispatch under the scope: kept as it is, nothing sent, the caller attached to it.
			created = [];
			placeholder = (h) => ({
				...h,
				dispatchId: "dsp_someone_elses",
				state: "unknown",
				errorCode: "PERMISSION_LOST",
				frames: [
					{ callId: h.callId, sequence: "0", type: "admitted" },
					{ callId: h.callId, sequence: "1", type: "error", outcome: "unknown", errorCode: "PERMISSION_LOST" },
				],
			});
			const foreign = id("foreign-dispatch");
			sends = up.receives.length;
			const f = await call(holder.url, "POST", "/api/v1/model-calls", requestBody(foreign));
			expect(f.status, f.text).toBe(200);
			expect(f.frames.at(-1)).toMatchObject({ type: "error", errorCode: "PERMISSION_LOST" });
			expect(created).toEqual([]);
			expect(up.receives.length).toBe(sends);
			expect((await holder.store.read(scope(foreign)))?.record).toMatchObject({
				dispatchId: "dsp_someone_elses",
				state: "unknown",
			});
			// A record of another caller under the scope: refused as a reentry is, never replaced, nothing sent.
			placeholder = (h) => ({
				...h,
				principalId: "anvilkit-job-access-sidecar",
				state: "unknown",
				errorCode: "PERMISSION_LOST",
			});
			const theirs = id("foreign-caller");
			sends = up.receives.length;
			const t = await call(holder.url, "POST", "/api/v1/model-calls", requestBody(theirs));
			expect(t.status).toBe(403);
			expect((t.json as { error: { code: string } }).error.code).toBe("FORBIDDEN");
			expect(created).toEqual([]);
			expect(up.receives.length).toBe(sends);
			expect((await holder.store.read(scope(theirs)))?.record.principalId).toBe("anvilkit-job-access-sidecar");
		} finally {
			await holder.close();
		}
	});

	it("a lost observation answer is resubmitted under the same source and sequence and applied once", async () => {
		control.loseObserveAnswers = 1;
		up.next({ kind: "stream", text: ["obs"] });
		const callId = id("observe-lost");
		const r = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(callId));
		expect(r.frames.at(-1)?.type).toBe("done");
		const dispatchId = control.byCall(callId)?.dispatchId;
		let mine: typeof control.observes = [];
		for (let i = 0; i < 100; i++) {
			mine = control.observes.filter((o) => o.dispatchId === dispatchId);
			if (mine.length >= 2 && !existsSync(path.join(proxy.storeDir, "pending", callId, "tenant_a"))) break;
			await new Promise((res) => setTimeout(res, 50));
		}
		expect(mine.length).toBe(2);
		expect(mine[0]?.source).toBe(mine[1]?.source);
		expect(mine[0]?.sequence).toBe(mine[1]?.sequence);
		expect(control.byCall(callId)?.observations).toHaveLength(1);
		const rec = await proxy.store.read(scope(callId));
		expect(rec?.record.observations.every((o) => o.submitted)).toBe(true);
	});

	it("authentication: no or unknown bearer is refused, the sidecar principal is the actor of its admissions, the probes live on the health listener", async () => {
		const none = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(id("noauth")), null);
		expect(none.status).toBe(401);
		const wrong = await call(proxy.url, "GET", `/api/v1/model-calls/x`, undefined, "not-a-token");
		expect(wrong.status).toBe(401);
		up.next({ kind: "stream", text: ["sidecar"] });
		const callId = id("sidecar");
		const r = await call(proxy.url, "POST", "/api/v1/model-calls", requestBody(callId), sidecarToken);
		expect(r.status).toBe(200);
		expect(control.admits.find((a) => a.callId === callId)?.command?.actorId).toBe("anvilkit-job-access-sidecar");
		expect((await call(proxy.url, "GET", "/healthz", undefined, null)).status).toBe(401);
		expect((await call(proxy.healthUrl, "GET", "/healthz", undefined, null)).status).toBe(200);
		expect((await call(proxy.healthUrl, "GET", "/readyz", undefined, null)).status).toBe(200);
		expect((await call(proxy.healthUrl, "GET", "/api/v1/model-calls", undefined, null)).status).toBe(404);
	});

	it("authorization: a call is read, canceled and reentered by its own caller (any replica) only; Control's trusted query may read it; a valid token with the call id grants nothing else", async () => {
		up.next({ kind: "stream-then-hang", text: ["mine"] });
		const callId = id("owned");
		const body = requestBody(callId);
		const sends = up.receives.length;
		const streaming = call(proxy.url, "POST", "/api/v1/model-calls", body);
		await up.awaitHeld();
		// An unrelated identity (the sidecar's) learns nothing and changes nothing.
		expect((await call(proxy.url, "GET", `/api/v1/model-calls/${callId}`, undefined, sidecarToken)).status).toBe(404);
		expect(
			(await call(proxy.url, "POST", `/api/v1/model-calls/${callId}/cancellations`, "{}", sidecarToken)).status,
		).toBe(404);
		const replay = await call(proxy.url, "POST", "/api/v1/model-calls", body, sidecarToken);
		expect(replay.status).toBe(403);
		expect((replay.json as { error: { code: string } }).error.code).toBe("FORBIDDEN");
		expect(await proxy.store.cancelRequested(scope(callId))).toBeUndefined();
		// Control's recovery query reads the record; it neither cancels nor reenters.
		const byControl = await call(proxy.url, "GET", `/api/v1/model-calls/${callId}`, undefined, controlToken);
		expect(byControl.status).toBe(200);
		expect(byControl.json).toMatchObject({ callId, state: "sending" });
		expect(
			(await call(proxy.url, "POST", `/api/v1/model-calls/${callId}/cancellations`, "{}", controlToken)).status,
		).toBe(404);
		expect((await call(proxy.url, "POST", "/api/v1/model-calls", body, controlToken)).status).toBe(403);
		expect(await proxy.store.cancelRequested(scope(callId))).toBeUndefined();
		// The caller's other replica (the same principal) attaches to the live send and cancels it.
		const other = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "replica",
			storeDir: proxy.storeDir,
		});
		try {
			expect((await call(other.url, "GET", `/api/v1/model-calls/${callId}`)).status).toBe(200);
			expect((await call(other.url, "POST", `/api/v1/model-calls/${callId}/cancellations`, "{}")).status).toBe(202);
			const s = await streaming;
			expect(s.frames.at(-1)).toMatchObject({ type: "error", outcome: "unknown", errorCode: "CANCELED" });
			const again = await call(other.url, "POST", "/api/v1/model-calls", body);
			expect(again.status).toBe(200);
			expect(again.frames).toEqual(s.frames);
		} finally {
			await other.close();
		}
		expect(up.receives.length).toBe(sends + 1);
		up.release();
	});

	it("malformed and out-of-contract requests are refused before any admission", async () => {
		const admits = control.admits.length;
		for (const [name, body, code] of [
			["duplicate member", `{"callId":"a","callId":"b"}`, "INVALID_ARGUMENT"],
			["provider key member", `${requestBody(id("key")).slice(0, -1)},"apiKey":"sk-x"}`, "INVALID_ARGUMENT"],
			["unknown route", requestBody(id("route"), { routeId: "openai-real" }), "PROFILE_UNQUALIFIED"],
			["float exposure", requestBody(id("float")).replace('"amount":"1000"', '"amount":0.25'), "INVALID_ARGUMENT"],
		] as const) {
			const r = await call(proxy.url, "POST", "/api/v1/model-calls", body);
			expect(r.status, name).toBeGreaterThanOrEqual(400);
			expect((r.json as { error: { code: string } }).error.code, name).toBe(code);
		}
		expect(control.admits.length).toBe(admits);
		const missing = await call(proxy.url, "GET", "/api/v1/model-calls/call_missing");
		expect(missing.status).toBe(404);
	});
});

describe("two Proxy instances on one store", () => {
	let up: Upstream;
	let control: FakeControl;
	let a: RunningProxy;
	let b: RunningProxy;
	beforeAll(async () => {
		up = await new Upstream().start();
		control = await new FakeControl().start();
		a = await startProxy({ upstreamUrl: up.url, controlAddress: control.address, instanceId: "a" });
		b = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "b",
			storeDir: a.storeDir,
		});
	});
	afterAll(async () => {
		await a.close();
		await b.close();
		await control.stop();
		await up.stop();
	});

	it("the same call on both instances is sent once and both callers receive the same frames", async () => {
		up.next({ kind: "stream", text: ["race"] });
		let release: () => void = () => {};
		const gate = new Promise<void>((r) => {
			release = r;
		});
		let held = 0;
		control.holdAdmits = async () => {
			held++;
			if (held >= 2) release();
			await gate;
		};
		const callId = `call_race_${Date.now()}`;
		const body = requestBody(callId);
		const [ra, rb] = await Promise.all([
			call(a.url, "POST", "/api/v1/model-calls", body),
			call(b.url, "POST", "/api/v1/model-calls", body),
		]);
		control.holdAdmits = undefined;
		expect(ra.status, ra.text).toBe(200);
		expect(rb.status, rb.text).toBe(200);
		expect(ra.frames).toEqual(rb.frames);
		expect(ra.frames.at(-1)?.type).toBe("done");
		expect(up.receives.length).toBe(1);
		expect(control.byCall(callId)?.observations).toHaveLength(1);
		expect(control.admits.filter((x) => x.callId === callId)).toHaveLength(2);
		const ga = await call(a.url, "GET", `/api/v1/model-calls/${callId}`);
		const gb = await call(b.url, "GET", `/api/v1/model-calls/${callId}`);
		expect(ga.json).toEqual(gb.json);
	});

	it("a cancel that reaches the other instance aborts the send", async () => {
		up.next({ kind: "stream-then-hang", text: ["hold"] });
		const callId = `call_xcancel_${Date.now()}`;
		const streaming = call(a.url, "POST", "/api/v1/model-calls", requestBody(callId));
		await up.awaitHeld();
		const c = await call(b.url, "POST", `/api/v1/model-calls/${callId}/cancellations`, "{}");
		expect(c.status).toBe(202);
		const s = await streaming;
		expect(s.frames.at(-1)).toMatchObject({ type: "error", outcome: "unknown", errorCode: "CANCELED" });
		up.release();
	});

	/** A record as the sender leaves it before its send (marker first, then the record). */
	const sendingRecord = (callId: string, tenantId: string, dispatchId: string, deadline: string) => {
		const now = new Date().toISOString();
		return {
			callId,
			tenantId,
			principalId: "anvilkit-agent-workflow",
			routeId: "controlled-openai-v1",
			provider: "fixture",
			model: "fixture-model",
			requestDigest: digest,
			contentDigest: digest,
			binding: { tenantId, operationId: "op_1", attemptId: "att_1", executionEpoch: "1" },
			deadline,
			maxExposure: { currency: "USD", amount: "1000" },
			maxOutputTokens: 1,
			dispatchId,
			state: "sending" as const,
			createdAt: now,
			updatedAt: now,
			revision: 1,
			frames: [],
			observations: [],
		};
	};
	const fakeDispatch = (callId: string, tenantId: string, dispatchId: string) => {
		control.dispatches.set(`${tenantId}/anvilkit-agent-model-proxy/${callId}`, {
			dispatchId,
			callId,
			owner: "anvilkit-agent-model-proxy",
			tenantId,
			digest: "x",
			state: DispatchState.DISPATCH_STATE_AUTHORIZED,
			outcome: DispatchOutcome.DISPATCH_OUTCOME_UNSPECIFIED,
			observations: [],
		});
	};
	const markerOf = (callId: string, tenantId = "tenant_a") => path.join(a.storeDir, "pending", callId, tenantId);
	const untilGone = async (file: string) => {
		for (let i = 0; i < 100 && existsSync(file); i++) await new Promise((r) => setTimeout(r, 50));
	};

	it("a settlement whose record write was lost is completed from the evidence by the other instance's sweep: the original outcome and usage, one observation, no send; repeated sweeps add nothing", async () => {
		const callId = `call_evidence_${Date.now()}`;
		const scope = { tenantId: "tenant_a", callId };
		fakeDispatch(callId, "tenant_a", "dsp_fake_evidence");
		const now = new Date().toISOString();
		const deadline = new Date(Date.now() + 60_000).toISOString();
		const usage = { inputUnits: "9", outputUnits: "4", reasoningUnits: "0", cachedInputUnits: "0" };
		const frames: StreamFrame[] = [
			{ callId, sequence: "0", type: "admitted" },
			{ callId, sequence: "1", type: "text", text: "settled" },
			{ callId, sequence: "2", type: "usage", usage },
			{ callId, sequence: "3", type: "done", outcome: "succeeded", usage },
		];
		// The sender's durable order up to its loss: marker, record (sending), evidence with the settlement.
		await a.store.markPending(scope, deadline);
		await a.store.create(sendingRecord(callId, "tenant_a", "dsp_fake_evidence", deadline));
		await a.store.writeEvidence({
			callId,
			tenantId: "tenant_a",
			dispatchId: "dsp_fake_evidence",
			routeId: "controlled-openai-v1",
			capturedAt: now,
			request: {
				method: "POST",
				url: `${up.url}/v1/chat/completions`,
				contentType: "application/json",
				body: "{}",
				bodyTruncated: false,
			},
			response: { status: 200, headers: {}, bodyBase64: "", bodyBytes: 0, bodyTruncated: false },
			settlement: { outcome: "succeeded", usage, nativeReference: "chatcmpl-settled", frames },
		});
		const sends = up.receives.length;
		const swept = await b.calls.sweep();
		expect(swept).toMatchObject({ completed: 1, reclaimed: 0 });
		await untilGone(markerOf(callId));
		expect(up.receives.length).toBe(sends);
		expect(control.byCall(callId)?.state).toBe(DispatchState.DISPATCH_STATE_OBSERVED);
		expect(control.byCall(callId)?.observations).toHaveLength(1);
		expect(control.byCall(callId)?.observations[0]).toMatchObject({
			source: "model-proxy/b",
			outcome: DispatchOutcome.DISPATCH_OUTCOME_SUCCEEDED,
			usage: { input: "9", output: "4" },
			nativeReference: "chatcmpl-settled",
		});
		const g = await call(a.url, "GET", `/api/v1/model-calls/${callId}`);
		expect(g.json).toMatchObject({ state: "succeeded", usage, nativeReference: "chatcmpl-settled" });
		expect((await a.store.read(scope))?.record.frames).toEqual(frames);
		const again = await a.calls.sweep();
		expect(again).toMatchObject({ completed: 0, reclaimed: 0, resubmitted: 0 });
		await b.calls.sweep();
		expect(control.byCall(callId)?.observations).toHaveLength(1);
		expect(up.receives.length).toBe(sends);
	});

	it("a pending marker without a record (the opener died between the marker and the record) is kept until the call's deadline plus grace, then cleared", async () => {
		const young = { tenantId: "tenant_a", callId: `call_marker_young_${Date.now()}` };
		const old = { tenantId: "tenant_a", callId: `call_marker_old_${Date.now()}` };
		await a.store.markPending(young, new Date(Date.now() + 60_000).toISOString());
		await a.store.markPending(old, new Date(Date.now() - 60_000).toISOString());
		await b.calls.sweep();
		expect(await a.store.listPending()).toContainEqual(young);
		expect(await a.store.listPending()).not.toContainEqual(old);
		await a.store.clearPending(young);
	});

	it("a send lost with its process is reclaimed as unknown by the sweep after the deadline, never resent; pending observations are resubmitted; the marker stays for the late-settlement window", async () => {
		const callId = `call_lost_${Date.now()}`;
		const scope = { tenantId: "tenant_a", callId };
		fakeDispatch(callId, "tenant_a", "dsp_fake_lost");
		const past = new Date(Date.now() - 2000).toISOString();
		await a.store.create(sendingRecord(callId, "tenant_a", "dsp_fake_lost", past));
		await a.store.markPending(scope, past);
		const sends = up.receives.length;
		const swept = await b.calls.sweep();
		expect(swept.reclaimed).toBe(1);
		for (let i = 0; i < 100 && !control.byCall(callId)?.observations.length; i++)
			await new Promise((r) => setTimeout(r, 50));
		expect(up.receives.length).toBe(sends);
		expect(control.byCall(callId)?.state).toBe(DispatchState.DISPATCH_STATE_UNKNOWN);
		expect(control.byCall(callId)?.observations[0]).toMatchObject({
			source: "model-proxy/b",
			outcome: DispatchOutcome.DISPATCH_OUTCOME_UNKNOWN,
		});
		const g = await call(a.url, "GET", `/api/v1/model-calls/${callId}`);
		expect(g.json).toMatchObject({ state: "unknown", errorCode: "SENDER_LOST" });
		const replay = await call(
			a.url,
			"POST",
			"/api/v1/model-calls",
			requestBody(callId, { deadline: new Date(Date.now() + 60_000).toISOString() }),
		);
		expect(replay.status).toBe(409);
		expect(up.receives.length).toBe(sends);
		// The observation is submitted, yet the marker stays: the lost sender's
		// evidence may still land. It goes once the late-settlement window passed.
		expect((await a.store.read(scope))?.record.observations.every((o) => o.submitted)).toBe(true);
		expect(existsSync(markerOf(callId))).toBe(true);
		await b.calls.sweep();
		expect(existsSync(markerOf(callId))).toBe(true);
		await new Promise((r) => setTimeout(r, a.cfg.observation.lateSettlementWindowMs + 100));
		await b.calls.sweep();
		expect(existsSync(markerOf(callId))).toBe(false);
		expect(control.byCall(callId)?.observations).toHaveLength(1);
	});

	it("evidence that lands after the reclaim, its sender lost before publishing, is completed by any instance's sweep: the actual settlement and usage under the original dispatch, no send; repeated sweeps and a lost observation receipt add no charge", async () => {
		const callId = `call_late_${Date.now()}`;
		const scope = { tenantId: "tenant_a", callId };
		fakeDispatch(callId, "tenant_a", "dsp_fake_late");
		const past = new Date(Date.now() - 2000).toISOString();
		await a.store.create(sendingRecord(callId, "tenant_a", "dsp_fake_late", past));
		await a.store.markPending(scope, past);
		const sends = up.receives.length;
		// The sweep reclaims the send as unknown and submits that; Control retains the exposure.
		expect((await b.calls.sweep()).reclaimed).toBe(1);
		for (let i = 0; i < 100 && !control.byCall(callId)?.observations.length; i++)
			await new Promise((r) => setTimeout(r, 50));
		expect(control.byCall(callId)?.state).toBe(DispatchState.DISPATCH_STATE_UNKNOWN);
		// The sender, slow rather than dead, persists its evidence with the
		// settlement and re-marks the call as the send path does — then dies
		// before publishing the record.
		const usage = { inputUnits: "21", outputUnits: "5", reasoningUnits: "0", cachedInputUnits: "0" };
		const frames: StreamFrame[] = [
			{ callId, sequence: "0", type: "admitted" },
			{ callId, sequence: "1", type: "text", text: "late" },
			{ callId, sequence: "2", type: "usage", usage },
			{ callId, sequence: "3", type: "done", outcome: "succeeded", usage },
		];
		await a.store.writeEvidence({
			callId,
			tenantId: "tenant_a",
			dispatchId: "dsp_fake_late",
			routeId: "controlled-openai-v1",
			capturedAt: new Date().toISOString(),
			request: {
				method: "POST",
				url: `${up.url}/v1/chat/completions`,
				contentType: "",
				body: "{}",
				bodyTruncated: false,
			},
			response: { status: 200, headers: {}, bodyBase64: "", bodyBytes: 0, bodyTruncated: false },
			settlement: { outcome: "succeeded", usage, nativeReference: "chatcmpl-late", frames },
		});
		await a.store.markPending(scope, past);
		// The other instance's sweep completes it; its first observation receipt is lost.
		control.loseObserveAnswers = 1;
		const swept = await a.calls.sweep();
		expect(swept).toMatchObject({ completed: 1, reclaimed: 0 });
		await untilGone(markerOf(callId));
		expect(existsSync(markerOf(callId))).toBe(false);
		expect(up.receives.length).toBe(sends);
		const rec = (await a.store.read(scope))?.record;
		expect(rec).toMatchObject({
			state: "succeeded",
			usage,
			nativeReference: "chatcmpl-late",
			dispatchId: "dsp_fake_late",
		});
		expect(rec?.frames).toEqual(frames);
		expect(rec?.observations.map((o) => [o.outcome, o.submitted])).toEqual([
			["unknown", true],
			["succeeded", true],
		]);
		const dispatch = control.byCall(callId);
		expect(dispatch?.state).toBe(DispatchState.DISPATCH_STATE_OBSERVED);
		expect(dispatch?.observations.map((o) => o.outcome)).toEqual([
			DispatchOutcome.DISPATCH_OUTCOME_UNKNOWN,
			DispatchOutcome.DISPATCH_OUTCOME_SUCCEEDED,
		]);
		expect(dispatch?.observations[1]).toMatchObject({ source: "model-proxy/a", usage: { input: "21", output: "5" } });
		// The lost receipt was resubmitted under the same source and sequence: one observation, not two.
		const submitted = control.observes.filter((o) => o.dispatchId === "dsp_fake_late");
		expect(submitted.length).toBe(3);
		expect(submitted[1]?.sequence).toBe(submitted[2]?.sequence);
		const g = await call(a.url, "GET", `/api/v1/model-calls/${callId}`);
		expect(g.json).toMatchObject({ state: "succeeded", usage, nativeReference: "chatcmpl-late" });
		for (const instance of [a, b]) {
			expect(await instance.calls.sweep()).toMatchObject({ completed: 0, reclaimed: 0, resubmitted: 0 });
		}
		expect(control.byCall(callId)?.observations).toHaveLength(2);
		expect(up.receives.length).toBe(sends);
		// Evidence that establishes nothing more than the reclaim keeps the
		// unknown exposure: no observation, and the marker is released.
		const vague = `call_vague_${Date.now()}`;
		const vagueScope = { tenantId: "tenant_a", callId: vague };
		fakeDispatch(vague, "tenant_a", "dsp_fake_vague");
		await a.store.create(sendingRecord(vague, "tenant_a", "dsp_fake_vague", past));
		await a.store.markPending(vagueScope, past);
		expect((await b.calls.sweep()).reclaimed).toBe(1);
		for (let i = 0; i < 100 && !control.byCall(vague)?.observations.length; i++)
			await new Promise((r) => setTimeout(r, 50));
		await a.store.writeEvidence({
			callId: vague,
			tenantId: "tenant_a",
			dispatchId: "dsp_fake_vague",
			routeId: "controlled-openai-v1",
			capturedAt: new Date().toISOString(),
			request: {
				method: "POST",
				url: `${up.url}/v1/chat/completions`,
				contentType: "",
				body: "{}",
				bodyTruncated: false,
			},
			transportError: "socket hang up",
			settlement: {
				outcome: "unknown",
				errorCode: "UPSTREAM_ERROR",
				frames: [
					{ callId: vague, sequence: "0", type: "admitted" },
					{ callId: vague, sequence: "1", type: "error", outcome: "unknown", errorCode: "UPSTREAM_ERROR" },
				],
			},
		});
		await a.store.markPending(vagueScope, past);
		expect((await b.calls.sweep()).completed).toBe(1);
		await untilGone(markerOf(vague));
		expect((await a.store.read(vagueScope))?.record).toMatchObject({
			state: "unknown",
			errorCode: "UPSTREAM_ERROR",
			evidenceRef: `evidence/${vague}/tenant_a`,
		});
		expect(control.byCall(vague)?.observations).toHaveLength(1);
		expect(control.byCall(vague)?.state).toBe(DispatchState.DISPATCH_STATE_UNKNOWN);
		expect(up.receives.length).toBe(sends);
	});

	it("evidence of another dispatch never settles a record: the call keeps its own outcome", async () => {
		const callId = `call_foreign_${Date.now()}`;
		const scope = { tenantId: "tenant_a", callId };
		fakeDispatch(callId, "tenant_a", "dsp_fake_own");
		const past = new Date(Date.now() - 2000).toISOString();
		await a.store.create(sendingRecord(callId, "tenant_a", "dsp_fake_own", past));
		await a.store.markPending(scope, past);
		await a.store.writeEvidence({
			callId,
			tenantId: "tenant_a",
			dispatchId: "dsp_fake_other",
			routeId: "controlled-openai-v1",
			capturedAt: new Date().toISOString(),
			request: { method: "POST", url: "u", contentType: "", body: "{}", bodyTruncated: false },
			settlement: {
				outcome: "succeeded",
				usage: { inputUnits: "1", outputUnits: "1", reasoningUnits: "0", cachedInputUnits: "0" },
				frames: [],
			},
		});
		const swept = await b.calls.sweep();
		expect(swept).toMatchObject({ completed: 0, reclaimed: 1 });
		for (let i = 0; i < 100 && !control.byCall(callId)?.observations.length; i++)
			await new Promise((r) => setTimeout(r, 50));
		expect((await a.store.read(scope))?.record).toMatchObject({ state: "unknown", errorCode: "SENDER_LOST" });
		expect(control.byCall(callId)?.state).toBe(DispatchState.DISPATCH_STATE_UNKNOWN);
	});

	it("two tenants under one call id are two calls: each principal's own admission, send, record, evidence, cancel and settlement; neither reads, moves or replaces the other", async () => {
		up.next({ kind: "stream", text: ["for tenant a"] }, { kind: "stream-then-hang", text: ["for tenant b"] });
		const callId = `call_shared_${Date.now()}`;
		const forA = requestBody(callId);
		const forB = requestBody(callId, {
			binding: { tenantId: "tenant_b", operationId: "op_9", attemptId: "att_9", executionEpoch: "1" },
			messages: [{ role: "user", content: "Something else entirely." }],
		});
		const sends = up.receives.length;
		const ra = await call(a.url, "POST", "/api/v1/model-calls", forA);
		expect(ra.status, ra.text).toBe(200);
		expect(ra.frames.at(-1)).toMatchObject({ type: "done", outcome: "succeeded" });
		// The other tenant's call, from the sidecar, on the other instance, while the first is settled.
		const streaming = call(b.url, "POST", "/api/v1/model-calls", forB, sidecarToken);
		await up.awaitHeld();
		expect(up.receives.length).toBe(sends + 2);
		const dispatchA = control.dispatches.get(`tenant_a/anvilkit-agent-model-proxy/${callId}`);
		const dispatchB = control.dispatches.get(`tenant_b/anvilkit-agent-model-proxy/${callId}`);
		expect(dispatchA?.dispatchId).toBeDefined();
		expect(dispatchB?.dispatchId).toBeDefined();
		expect(dispatchA?.dispatchId).not.toBe(dispatchB?.dispatchId);
		// Each principal reads its own; a query by the id alone never crosses tenants.
		const ga = await call(a.url, "GET", `/api/v1/model-calls/${callId}`);
		expect(ga.json).toMatchObject({ callId, state: "succeeded", dispatchId: dispatchA?.dispatchId });
		const gb = await call(b.url, "GET", `/api/v1/model-calls/${callId}`, undefined, sidecarToken);
		expect(gb.json).toMatchObject({ callId, state: "sending", dispatchId: dispatchB?.dispatchId });
		// A cancel by the workflow principal lands on nothing of tenant b's send.
		const ca = await call(a.url, "POST", `/api/v1/model-calls/${callId}/cancellations`, "{}");
		expect(ca.status).toBe(202);
		expect(ca.json).toMatchObject({ dispatchId: dispatchA?.dispatchId, state: "succeeded" });
		expect(await a.store.cancelRequested({ tenantId: "tenant_b", callId })).toBeUndefined();
		// A reentry of tenant a's call from the sidecar is refused (another caller), tenant b's send is untouched.
		expect((await call(b.url, "POST", "/api/v1/model-calls", forA, sidecarToken)).status).toBe(403);
		expect(up.receives.length).toBe(sends + 2);
		// The sidecar cancels its own; the workflow's record stays as it was.
		const cb = await call(a.url, "POST", `/api/v1/model-calls/${callId}/cancellations`, "{}", sidecarToken);
		expect(cb.status).toBe(202);
		expect(cb.json).toMatchObject({ dispatchId: dispatchB?.dispatchId });
		const rb = await streaming;
		expect(rb.frames.at(-1)).toMatchObject({ type: "error", outcome: "unknown", errorCode: "CANCELED" });
		up.release();
		const recA = (await a.store.read({ tenantId: "tenant_a", callId }))?.record;
		const recB = (await a.store.read({ tenantId: "tenant_b", callId }))?.record;
		expect(recA).toMatchObject({
			state: "succeeded",
			principalId: "anvilkit-agent-workflow",
			dispatchId: dispatchA?.dispatchId,
		});
		expect(recA?.cancelRequestedAt).toBeUndefined();
		expect(recB).toMatchObject({
			state: "unknown",
			principalId: "anvilkit-job-access-sidecar",
			dispatchId: dispatchB?.dispatchId,
		});
		expect(recA?.evidenceRef).toBe(`evidence/${callId}/tenant_a`);
		expect(recB?.evidenceRef).toBe(`evidence/${callId}/tenant_b`);
		expect(dispatchA?.observations).toHaveLength(1);
		expect(dispatchB?.observations).toHaveLength(1);
		expect(dispatchA?.observations[0]?.outcome).toBe(DispatchOutcome.DISPATCH_OUTCOME_SUCCEEDED);
		expect(dispatchB?.observations[0]?.outcome).toBe(DispatchOutcome.DISPATCH_OUTCOME_UNKNOWN);
		// Control's trusted query, by id alone, cannot name the tenant among two: it is answered nothing.
		expect((await call(a.url, "GET", `/api/v1/model-calls/${callId}`, undefined, controlToken)).status).toBe(404);
		// The same principal opening the id for a third tenant is a third call — and its own queries by the
		// id alone are answered nothing rather than one of its two, while the binding still names each.
		up.next({ kind: "stream", text: ["for tenant c"] });
		const forC = requestBody(callId, {
			binding: { tenantId: "tenant_c", operationId: "op_3", attemptId: "att_3", executionEpoch: "1" },
		});
		const rc = await call(a.url, "POST", "/api/v1/model-calls", forC);
		expect(rc.status, rc.text).toBe(200);
		expect(rc.frames.at(-1)).toMatchObject({ type: "done", outcome: "succeeded" });
		expect(up.receives.length).toBe(sends + 3);
		expect((await call(a.url, "GET", `/api/v1/model-calls/${callId}`)).status).toBe(404);
		expect((await call(a.url, "POST", "/api/v1/model-calls", forA)).frames).toEqual(ra.frames);
		expect((await call(b.url, "POST", "/api/v1/model-calls", forC)).frames).toEqual(rc.frames);
		expect(up.receives.length).toBe(sends + 3);
		expect(await a.store.listCalls(callId)).toHaveLength(3);
	});

	it("the permission holder takes over only the placeholder a duplicate recorded under the same identity; another identity's record under the scope is never overwritten", async () => {
		// A lost first answer on instance a records PERMISSION_LOST; the same
		// request reentered on instance b is answered that record, never a send.
		control.loseAdmitAnswers = 1;
		const lostId = `call_placeholder_${Date.now()}`;
		const body = requestBody(lostId);
		const sends = up.receives.length;
		const first = await call(a.url, "POST", "/api/v1/model-calls", body);
		expect(first.status, first.text).toBe(200);
		expect(first.frames.at(-1)).toMatchObject({ type: "error", outcome: "unknown", errorCode: "PERMISSION_LOST" });
		const again = await call(b.url, "POST", "/api/v1/model-calls", body);
		expect(again.frames).toEqual(first.frames);
		expect(up.receives.length).toBe(sends);
		const placeholder = (await a.store.read({ tenantId: "tenant_a", callId: lostId }))?.record;
		expect(placeholder).toMatchObject({ state: "unknown", errorCode: "PERMISSION_LOST" });
		// Another caller's request under the same scope neither takes it over
		// nor sends: the record is that of its own caller, as recorded.
		const other = await call(b.url, "POST", "/api/v1/model-calls", body, sidecarToken);
		expect(other.status).toBe(403);
		expect((await a.store.read({ tenantId: "tenant_a", callId: lostId }))?.record).toEqual(placeholder);
		expect(up.receives.length).toBe(sends);
	});
});
