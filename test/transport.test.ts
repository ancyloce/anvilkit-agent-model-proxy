import { encode } from "eventsource-encoder";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { contextOf, streamUpstream, type UpstreamEvent } from "../src/adapters/piai.js";
import { nativeUsageFromSse, refusingFetch, SendTicket, TransportRefused } from "../src/adapters/transport.js";
import type { Route } from "../src/config.js";
import { Upstream } from "./upstream.js";

const credential = "fixture-credential";

function route(baseUrl: string): Route {
	return {
		id: "controlled-openai-v1",
		enabled: true,
		api: "openai-completions",
		provider: "fixture",
		model: "fixture-model",
		baseUrl,
		credentialEnv: "ANVILKIT_MODEL_PROXY_CREDENTIAL_CONTROLLED_OPENAI_V1",
		limits: {
			maxOutputTokens: 4096,
			maxExposure: { currency: "USD", amount: "1000000" },
			maxDeadlineMs: 600_000,
			maxFrameBytes: 65536,
			maxOutputBytes: 1_048_576,
			maxFrames: 4096,
			upstreamTimeoutMs: 300_000,
			maxEvidenceBytes: 4_194_304,
		},
		tools: [
			{
				name: "read_brief",
				description: "Reads the frozen brief.",
				inputSchema: { type: "object", additionalProperties: false, properties: { section: { type: "string" } } },
			},
		],
	};
}

async function collect(gen: AsyncGenerator<UpstreamEvent>): Promise<UpstreamEvent[]> {
	const out: UpstreamEvent[] = [];
	for await (const ev of gen) out.push(ev);
	return out;
}

describe("ticket-bound transport", () => {
	let up: Upstream;
	beforeAll(async () => {
		up = await new Upstream().start();
	});
	afterAll(async () => {
		await up.stop();
	});

	it("the process-wide fetch of the bootstrap refuses every request and counts nothing upstream", async () => {
		const before = up.receives.length;
		await expect(refusingFetch(`${up.url}/v1/chat/completions`)).rejects.toBeInstanceOf(TransportRefused);
		expect(up.receives.length).toBe(before);
	});

	it("allows exactly one request per ticket, on the route origin, with the route credential", async () => {
		const before = up.receives.length;
		const ticket = new SendTicket("call_t1", `${up.url}/v1`, credential, 4096);
		const headers = { authorization: `Bearer ${credential}`, "content-type": "application/json" };
		const res = await ticket.fetch(`${up.url}/v1/chat/completions`, {
			method: "POST",
			headers,
			body: JSON.stringify({ stream: true }),
		});
		expect(res.status).toBe(200);
		await res.text();
		await expect(
			ticket.fetch(`${up.url}/v1/chat/completions`, { method: "POST", headers, body: "{}" }),
		).rejects.toThrow(/already used/);
		expect(up.receives.length).toBe(before + 1);
		expect(ticket.refused).toEqual([`second request to ${new URL(up.url).origin}/v1/chat/completions`]);
		await ticket.settled;
		expect(ticket.request?.body).toBe('{"stream":true}');
		expect(ticket.response?.status).toBe(200);
		expect(ticket.response?.bodyBytes).toBeGreaterThan(0);

		for (const [name, url, auth] of [
			["another origin", `${up.targetUrl}/v1/chat/completions`, `Bearer ${credential}`],
			["a path outside the route", `${up.url}/admin`, `Bearer ${credential}`],
			["a forwarded caller token", `${up.url}/v1/chat/completions`, "Bearer inbound-anvilkit-token"],
			["no credential", `${up.url}/v1/chat/completions`, ""],
		] as const) {
			const t = new SendTicket("call_t2", `${up.url}/v1`, credential, 4096);
			const h: Record<string, string> = { "content-type": "application/json" };
			if (auth) h.authorization = auth;
			await expect(t.fetch(url, { method: "POST", headers: h, body: "{}" }), name).rejects.toBeInstanceOf(
				TransportRefused,
			);
			expect(t.used, name).toBe(false);
		}
		const t = new SendTicket("call_t3", `${up.url}/v1`, credential, 4096);
		await expect(
			t.fetch(`${up.url}/v1/chat/completions`, {
				method: "POST",
				headers: { authorization: `Bearer ${credential}`, "x-anvilkit-tenant": "tenant_a" },
				body: "{}",
			}),
		).rejects.toThrow(/forbidden header x-anvilkit-tenant/);
		expect(up.receives.length).toBe(before + 1);
		expect(up.redirectTargetReceives.length).toBe(0);
	});

	it("never follows a redirect: the redirect target receives nothing and the ticket stays used", async () => {
		up.next({ kind: "redirect", location: `${up.targetUrl}/v1/chat/completions` });
		const before = up.receives.length;
		const ticket = new SendTicket("call_t4", `${up.url}/v1`, credential, 4096);
		const res = await ticket.fetch(`${up.url}/v1/chat/completions`, {
			method: "POST",
			headers: { authorization: `Bearer ${credential}` },
			body: "{}",
		});
		expect(res.status).toBe(307);
		expect(up.receives.length).toBe(before + 1);
		expect(up.redirectTargetReceives.length).toBe(0);
		expect(ticket.used).toBe(true);
	});

	it("reads native usage and the response id from the captured body as an event stream: single-line, multiline, fragmented and multibyte inputs agree", () => {
		const usageChunk = { id: "chatcmpl-1", choices: [], usage: { prompt_tokens: 3, completion_tokens: 1 } };
		const textChunk = { id: "chatcmpl-1", choices: [{ delta: { content: "héllo 🎉 日本" } }] };
		const single = new TextEncoder().encode(
			encode({ data: JSON.stringify(textChunk) }) +
				encode({ data: JSON.stringify(usageChunk) }) +
				encode({ data: "[DONE]" }),
		);
		const expected = { usage: { prompt_tokens: 3, completion_tokens: 1 }, responseId: "chatcmpl-1" };
		expect(nativeUsageFromSse(single)).toEqual(expected);
		// The same events with the usage payload spread over several data lines (joined by newlines per the SSE algorithm).
		const multiline = new TextEncoder().encode(
			`${encode({ data: JSON.stringify(textChunk) })}data: {"id":"chatcmpl-1",\ndata: "choices":[],\ndata: "usage":{"prompt_tokens":3,"completion_tokens":1}}\n\n${encode({ data: "[DONE]" })}`,
		);
		expect(nativeUsageFromSse(multiline)).toEqual(expected);
		// CRLF line endings and a comment (keepalive) in between.
		const crlf = new TextEncoder().encode(
			`data: ${JSON.stringify(textChunk)}\r\n\r\n: keepalive\r\n\r\ndata: ${JSON.stringify(usageChunk)}\r\n\r\ndata: [DONE]\r\n\r\n`,
		);
		expect(nativeUsageFromSse(crlf)).toEqual(expected);
		// A body captured in fragments is the same bytes; a multibyte sequence cut by the capture boundary is one text.
		const fragments = [single.subarray(0, 41), single.subarray(41, 42), single.subarray(42)];
		const joined = new Uint8Array(single.byteLength);
		let off = 0;
		for (const f of fragments) {
			joined.set(f, off);
			off += f.byteLength;
		}
		expect(nativeUsageFromSse(joined)).toEqual(expected);
		// No usage chunk: absent, never zero.
		expect(
			nativeUsageFromSse(
				new TextEncoder().encode(encode({ data: JSON.stringify(textChunk) }) + encode({ data: "[DONE]" })),
			),
		).toBeUndefined();
		// A usage chunk whose event block was never terminated (truncated body) is not an event.
		expect(
			nativeUsageFromSse(
				new TextEncoder().encode(`${encode({ data: JSON.stringify(textChunk) })}data: ${JSON.stringify(usageChunk)}`),
			),
		).toBeUndefined();
		// What was reported is handed over as reported (the domain classifies it).
		expect(
			nativeUsageFromSse(
				new TextEncoder().encode(encode({ data: JSON.stringify({ id: "x", choices: [], usage: {} }) })),
			),
		).toEqual({ usage: {}, responseId: "x" });
	});
});

describe("pi-ai openai-completions path on the ticket's fetch", () => {
	let up: Upstream;
	beforeAll(async () => {
		up = await new Upstream().start();
	});
	afterAll(async () => {
		await up.stop();
	});

	function input(ticket: SendTicket, overrides: Partial<Parameters<typeof streamUpstream>[0]> = {}) {
		return {
			route: route(`${up.url}/v1`),
			credential,
			messages: [
				{ role: "system" as const, content: "You are the planner." },
				{ role: "user" as const, content: "Plan the hero component." },
			],
			tools: [
				{
					name: "read_brief",
					inputSchemaDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
				},
			],
			maxOutputTokens: 256,
			timeoutMs: 5_000,
			signal: new AbortController().signal,
			ticket,
			...overrides,
		};
	}

	it("sends once and re-expresses text, tool calls and completion; the wire carries only the route credential", async () => {
		up.next({
			kind: "stream",
			text: ["Hello", " world"],
			toolCall: { id: "call_abc", name: "read_brief", arguments: '{"section":"hero"}' },
		});
		const before = up.receives.length;
		const ticket = new SendTicket("call_p1", `${up.url}/v1`, credential, 1 << 20);
		const events = await collect(streamUpstream(input(ticket)));
		expect(up.receives.length).toBe(before + 1);
		const r = up.receives.at(-1);
		if (!r) throw new Error("receive");
		expect(r.url).toBe("/v1/chat/completions");
		expect(r.headers.authorization).toBe(`Bearer ${credential}`);
		expect(r.headers["user-agent"]).toBe("anvilkit-agent-model-proxy");
		expect(Object.keys(r.headers).some((h) => h.startsWith("x-anvilkit-"))).toBe(false);
		const body = JSON.parse(r.body) as Record<string, unknown>;
		expect(body.model).toBe("fixture-model");
		expect(body.stream).toBe(true);
		expect(body.max_tokens).toBe(256);
		expect(body.stream_options).toEqual({ include_usage: true });
		expect(body.prompt_cache_key).toBeUndefined();
		expect(body.store).toBeUndefined();
		expect((body.messages as unknown[]).length).toBe(2);
		expect((body.tools as { function: { name: string } }[])[0]?.function.name).toBe("read_brief");
		expect(events.map((e) => e.type)).toEqual(["text", "text", "tool_call", "done"]);
		expect(
			events
				.filter((e) => e.type === "text")
				.map((e) => (e as { delta: string }).delta)
				.join(""),
		).toBe("Hello world");
		const tc = events[2] as Extract<UpstreamEvent, { type: "tool_call" }>;
		expect(tc).toMatchObject({ id: "call_abc", name: "read_brief", arguments: '{"section":"hero"}' });
		await ticket.settled;
		expect(ticket.refused).toEqual([]);
		const native = nativeUsageFromSse(ticket.response?.body ?? new Uint8Array());
		expect(native?.usage).toMatchObject({ prompt_tokens: 12, completion_tokens: 5 });
		expect(native?.responseId).toBe("chatcmpl-fixture");
	});

	it("replays an assistant tool call and its tool result as the round trip the protocol states", async () => {
		up.next({ kind: "stream", text: ["The hero section."] });
		const before = up.receives.length;
		const ticket = new SendTicket("call_p1b", `${up.url}/v1`, credential, 1 << 20);
		const events = await collect(
			streamUpstream(
				input(ticket, {
					messages: [
						{ role: "system", content: "You are the planner." },
						{ role: "user", content: "Plan the hero component." },
						{
							role: "assistant",
							content: "",
							toolCalls: [{ toolCallId: "call_abc", name: "read_brief", arguments: '{"section":"hero"}' }],
						},
						{ role: "tool", toolCallId: "call_abc", content: "The brief says: one hero, one call to action." },
					],
				}),
			),
		);
		expect(events.map((e) => e.type)).toEqual(["text", "done"]);
		expect(up.receives.length).toBe(before + 1);
		const body = JSON.parse(up.receives.at(-1)?.body ?? "{}") as { messages: Record<string, unknown>[] };
		expect(body.messages).toHaveLength(4);
		expect(body.messages[2]).toMatchObject({
			role: "assistant",
			tool_calls: [
				{ id: "call_abc", type: "function", function: { name: "read_brief", arguments: '{"section":"hero"}' } },
			],
		});
		expect(body.messages[3]).toEqual({
			role: "tool",
			tool_call_id: "call_abc",
			content: "The brief says: one hero, one call to action.",
		});
	});

	it("the context keeps assistant text beside tool calls and names tool results by the call they answer", () => {
		const ctx = contextOf(
			route("http://127.0.0.1:1/v1"),
			[
				{ role: "system", content: "s" },
				{
					role: "assistant",
					content: "Reading the brief.",
					toolCalls: [{ toolCallId: "tc_1", name: "read_brief", arguments: '{"section":"hero"}' }],
				},
				{ role: "tool", toolCallId: "tc_1", content: "ok" },
				{ role: "assistant", content: "Done." },
			],
			route("http://127.0.0.1:1/v1").tools,
		);
		expect(ctx.systemPrompt).toBe("s");
		expect(ctx.messages[0]).toMatchObject({
			role: "assistant",
			provider: "fixture",
			model: "fixture-model",
			stopReason: "toolUse",
			content: [
				{ type: "text", text: "Reading the brief." },
				{ type: "toolCall", id: "tc_1", name: "read_brief", arguments: { section: "hero" } },
			],
		});
		expect(ctx.messages[1]).toMatchObject({
			role: "toolResult",
			toolCallId: "tc_1",
			toolName: "read_brief",
			isError: false,
		});
		expect(ctx.messages[2]).toMatchObject({
			role: "assistant",
			stopReason: "stop",
			content: [{ type: "text", text: "Done." }],
		});
	});

	it("retryable upstream failures reach the upstream once: the SDK's retries are off and the ticket would refuse them", async () => {
		for (const [name, behavior] of [
			["a 500", { kind: "status" as const, status: 500 }],
			["a 429", { kind: "status" as const, status: 429 }],
			["a 503", { kind: "status" as const, status: 503 }],
		] as const) {
			up.next(behavior);
			const before = up.receives.length;
			const ticket = new SendTicket("call_p2", `${up.url}/v1`, credential, 1 << 20);
			const events = await collect(streamUpstream(input(ticket)));
			expect(
				events.map((e) => e.type),
				name,
			).toEqual(["error"]);
			expect(up.receives.length, name).toBe(before + 1);
			await ticket.settled;
			expect(ticket.refused, name).toEqual([]);
			expect(ticket.response?.status, name).toBe(behavior.status);
		}
	});

	it("a redirect is a failed send, not a second one", async () => {
		up.next({ kind: "redirect", location: `${up.targetUrl}/v1/chat/completions` });
		const before = up.receives.length;
		const ticket = new SendTicket("call_p3", `${up.url}/v1`, credential, 1 << 20);
		const events = await collect(streamUpstream(input(ticket)));
		expect(events.map((e) => e.type)).toEqual(["error"]);
		expect(up.receives.length).toBe(before + 1);
		expect(up.redirectTargetReceives.length).toBe(0);
		await ticket.settled;
		expect(ticket.response?.status).toBe(307);
	});

	it("a dropped connection and a hung upstream end as errors after one attempt", async () => {
		up.next({ kind: "close" });
		const before = up.receives.length;
		const closed = new SendTicket("call_p4", `${up.url}/v1`, credential, 1 << 20);
		const events = await collect(streamUpstream(input(closed)));
		expect(events.map((e) => e.type)).toEqual(["error"]);
		expect(up.receives.length).toBe(before + 1);
		await closed.settled;
		expect(closed.transportError).toMatch(/fetch failed|socket|ECONNRESET|terminated/i);
		expect(closed.refused).toEqual([]);

		up.next({ kind: "hang" });
		const hung = new SendTicket("call_p5", `${up.url}/v1`, credential, 1 << 20);
		const t0 = Date.now();
		const hungEvents = await collect(streamUpstream(input(hung, { timeoutMs: 500 })));
		expect(hungEvents.map((e) => e.type)).toEqual(["error"]);
		expect(Date.now() - t0).toBeLessThan(5_000);
		expect(up.receives.length).toBe(before + 2);
		up.release();
		await hung.settled;
		expect(hung.refused).toEqual([]);
	});

	it("an aborted stream keeps what was captured and ends as aborted", async () => {
		up.next({ kind: "stream-then-hang", text: ["partial"] });
		const before = up.receives.length;
		const ticket = new SendTicket("call_p6", `${up.url}/v1`, credential, 1 << 20);
		const ac = new AbortController();
		const gen = streamUpstream(input(ticket, { signal: ac.signal }));
		const first = await gen.next();
		expect(first.value).toEqual({ type: "text", delta: "partial" });
		ac.abort();
		const rest: UpstreamEvent[] = [];
		for await (const ev of gen) rest.push(ev);
		expect(rest.map((e) => e.type)).toEqual(["error"]);
		expect((rest[0] as Extract<UpstreamEvent, { type: "error" }>).reason).toBe("aborted");
		expect(up.receives.length).toBe(before + 1);
		up.release();
		await ticket.settled;
		expect(nativeUsageFromSse(ticket.response?.body ?? new Uint8Array())).toBeUndefined();
	});
});
