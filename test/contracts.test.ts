import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { Contract, ContractViolation, contractsDir, type StreamFrame } from "../src/contracts.js";
import {
	canonicalJson,
	digestOf,
	encodedFrameBytes,
	Frames,
	parseTimestampMicros,
	timestamp,
	toolSchemaDigest,
	usageFromOpenAI,
} from "../src/domain/call.js";

interface Case {
	name: string;
	schema: string;
	valid: boolean;
	instance?: unknown;
	raw?: string;
}

describe("contract boundary", () => {
	const contract = new Contract();
	const fixtures = JSON.parse(
		readFileSync(path.join(contractsDir(), "openapi", "model-proxy.fixtures.json"), "utf8"),
	) as { cases: Case[] };

	it("agrees with every cross-runtime vector of openapi/model-proxy.fixtures.json", () => {
		expect(fixtures.cases.length).toBeGreaterThan(10);
		for (const c of fixtures.cases) {
			const text = c.raw ?? JSON.stringify(c.instance);
			let accepted = true;
			try {
				contract.parse(c.schema, text);
			} catch (err) {
				if (!(err instanceof ContractViolation)) throw err;
				accepted = false;
			}
			expect(accepted, c.name).toBe(c.valid);
		}
	});

	it("rejects duplicate members, lossy numbers and unknown members before the handler", () => {
		expect(() =>
			contract.parse(
				"Usage",
				'{"inputUnits":"1","inputUnits":"2","outputUnits":"0","reasoningUnits":"0","cachedInputUnits":"0"}',
			),
		).toThrow(/malformed/);
		// The shared strict parser of the contracts package decides, equal values included.
		expect(() =>
			contract.parse(
				"Usage",
				'{"inputUnits":"1","inputUnits":"1","outputUnits":"0","reasoningUnits":"0","cachedInputUnits":"0"}',
			),
		).toThrow(/malformed JSON: Duplicate key 'inputUnits'/);
		expect(() =>
			contract.parse(
				"StreamFrame",
				'{"callId":"c","sequence":"1","type":"usage","usage":{"inputUnits":"1","inputUnits":"1"}}',
			),
		).toThrow(/malformed JSON: Duplicate key 'inputUnits'/);
		expect(() =>
			contract.parse("StreamFrame", '{"callId":"c","sequence":"0","type":"admitted"} {"callId":"c"}'),
		).toThrow(/malformed JSON/);
		expect(() => contract.parse("ModelCallRequest", '{"maxOutputTokens":1e400}')).toThrow(/malformed/);
		expect(() =>
			contract.parse("StreamFrame", '{"callId":"c","sequence":"1","type":"text","text":"a","apiKey":"x"}'),
		).toThrow(/additional properties/);
	});
});

describe("domain", () => {
	it("bounds the frame sequence in encoded UTF-8 bytes, reserves the final frame's slot and closes once", () => {
		// The envelope of a text frame of call_1 with a one-digit sequence is 54 bytes: a
		// 58-byte bound leaves four bytes of escaped text per frame.
		const envelope = encodedFrameBytes({ callId: "call_1", sequence: "9", type: "text", text: "" });
		const f = new Frames("call_1", { maxFrameBytes: envelope + 4, maxOutputBytes: 10, maxFrames: 8 });
		f.admitted();
		f.text("abcdefghij");
		expect(f.frames.map((x) => x.text)).toEqual([undefined, "abcd", "efgh", "ij"]);
		for (const x of f.frames) expect(encodedFrameBytes(x)).toBeLessThanOrEqual(envelope + 4);
		expect(() => f.text("k")).toThrow(/OUTPUT_BOUND_EXCEEDED/);
		// The final frame is bounded by the contract, not by the route's frame bound: it always fits.
		f.done({ inputUnits: "1", outputUnits: "2", reasoningUnits: "0", cachedInputUnits: "0" });
		expect(f.frames.at(-1)?.sequence).toBe("4");
		expect(encodedFrameBytes(f.frames.at(-1) as StreamFrame)).toBeGreaterThan(envelope + 4);
		expect(() => f.text("x")).toThrow(/final frame/);
		// Multibyte text and JSON escapes count as encoded: a code point is never split.
		const m = new Frames("call_1", { maxFrameBytes: envelope + 6, maxOutputBytes: 1024, maxFrames: 8 });
		m.admitted();
		m.text('日本"語');
		// 日本 is six bytes; the escaped quote (two bytes) and 語 (three) share the next frame.
		expect(m.frames.map((x) => x.text)).toEqual([undefined, "日本", '"語']);
		for (const x of m.frames) expect(encodedFrameBytes(x)).toBeLessThanOrEqual(envelope + 6);
		// The last slot belongs to the final frame: content cannot take it, the final frame can.
		const g = new Frames("call_2", { maxFrameBytes: 65536, maxOutputBytes: 1024, maxFrames: 2 });
		g.admitted();
		expect(() => g.text("a")).toThrow(/SEQUENCE_BOUND_EXCEEDED/);
		g.error("failed", "SEQUENCE_BOUND_EXCEEDED");
		expect(g.frames.map((x) => x.type)).toEqual(["admitted", "error"]);
		// The usage frame yields when no content slot is left; the done frame still carries the usage.
		const u = new Frames("call_3", { maxFrameBytes: 65536, maxOutputBytes: 1024, maxFrames: 3 });
		u.admitted();
		u.text("a");
		u.usage({ inputUnits: "1", outputUnits: "1", reasoningUnits: "0", cachedInputUnits: "0" });
		u.done({ inputUnits: "1", outputUnits: "1", reasoningUnits: "0", cachedInputUnits: "0" });
		expect(u.frames.map((x) => x.type)).toEqual(["admitted", "text", "done"]);
		// Tool arguments are bounded by the contract's argument bound and by the encoded frame bound.
		const t = new Frames("call_4", { maxFrameBytes: 1024, maxOutputBytes: 1 << 20, maxFrames: 8 });
		t.admitted();
		expect(() => t.toolCall("tc", "read_brief", JSON.stringify({ x: "y".repeat(1100) }))).toThrow(
			/FRAME_BOUND_EXCEEDED/,
		);
		expect(() => t.toolCall("tc", "read_brief", "{}")).not.toThrow();
	});

	it("classifies native usage against the protocol and digests canonically", () => {
		expect(
			usageFromOpenAI({
				prompt_tokens: 120,
				completion_tokens: 34,
				total_tokens: 154,
				prompt_tokens_details: { cached_tokens: 20, audio_tokens: 0 },
				completion_tokens_details: { reasoning_tokens: 4, accepted_prediction_tokens: 0 },
			}),
		).toEqual({
			kind: "complete",
			usage: { inputUnits: "120", outputUnits: "34", reasoningUnits: "4", cachedInputUnits: "20" },
		});
		expect(usageFromOpenAI({ prompt_tokens: 0, completion_tokens: 0 })).toEqual({
			kind: "complete",
			usage: { inputUnits: "0", outputUnits: "0", reasoningUnits: "0", cachedInputUnits: "0" },
		});
		expect(usageFromOpenAI({ prompt_tokens: 3, completion_tokens: 2, prompt_tokens_details: null })).toMatchObject({
			kind: "complete",
		});
		for (const raw of [
			{},
			null,
			"usage",
			[3, 2],
			{ prompt_tokens: 3 },
			{ completion_tokens: 3 },
			{ prompt_tokens: -3, completion_tokens: 2 },
			{ prompt_tokens: 3.5, completion_tokens: 2 },
			{ prompt_tokens: "3", completion_tokens: 2 },
			{ prompt_tokens: 3, completion_tokens: Number.MAX_SAFE_INTEGER + 2 },
			{ prompt_tokens: 3, completion_tokens: 2, total_tokens: 6 },
			{ prompt_tokens: 3, completion_tokens: 2, total_tokens: "5" },
			{ prompt_tokens: 3, completion_tokens: 2, prompt_tokens_details: { cached_tokens: 4 } },
			{ prompt_tokens: 3, completion_tokens: 2, prompt_tokens_details: { cached_tokens: -1 } },
			{ prompt_tokens: 3, completion_tokens: 2, completion_tokens_details: { reasoning_tokens: 3 } },
			{ prompt_tokens: 3, completion_tokens: 2, completion_tokens_details: [] },
		]) {
			expect(usageFromOpenAI(raw).kind, JSON.stringify(raw)).toBe("invalid");
		}
		expect(canonicalJson({ b: [1, { d: null, c: "x" }], a: true })).toBe('{"a":true,"b":[1,{"c":"x","d":null}]}');
		expect(toolSchemaDigest({ type: "object", properties: { a: { type: "string" } } })).toBe(
			digestOf('{"properties":{"a":{"type":"string"}},"type":"object"}'),
		);
	});

	it("stamps strictly increasing contract timestamps", () => {
		const a = timestamp(1_700_000_000_000);
		const b = timestamp(1_700_000_000_000, a);
		expect(a).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/);
		expect(parseTimestampMicros(b)).toBe((parseTimestampMicros(a) ?? 0n) + 1n);
	});
});
