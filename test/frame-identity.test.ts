// D4a (DEC-070, design 0001 §8.2): the stream identity of the frames — the
// negotiation, the recovery pair, the decoration and the reserved bytes.
import { describe, expect, it } from "vitest";
import type { StreamFrame } from "../src/contracts.js";
import {
	encodedFrameBytes,
	Frames,
	frameIdentityBytes,
	isRecoveryFrames,
	newOriginalStreamId,
	withIdentity,
} from "../src/domain/call.js";
import { asksFrameIdentity } from "../src/transport/http.js";

describe("frame identity", () => {
	it("is asked for only by a text/event-stream range with frame-identity=1", () => {
		for (const accept of [
			"text/event-stream; frame-identity=1",
			"text/event-stream;frame-identity=1",
			"TEXT/Event-Stream; Frame-Identity = 1",
			"application/json, text/event-stream; charset=utf-8; frame-identity=1",
		]) {
			expect(asksFrameIdentity(accept), accept).toBe(true);
		}
		for (const accept of [
			undefined,
			"",
			"*/*",
			"text/event-stream",
			"text/event-stream; frame-identity=0",
			"text/event-stream; frame-identity=11",
			"application/json; frame-identity=1",
		]) {
			expect(asksFrameIdentity(accept), String(accept)).toBe(false);
		}
	});

	it("a recorded list is recovery only when it ends with the synthetic SENDER_LOST or PERMISSION_LOST error", () => {
		const f = (type: StreamFrame["type"], errorCode?: string): StreamFrame => ({
			callId: "c",
			sequence: "1",
			type,
			...(errorCode ? { outcome: "unknown" as const, errorCode } : {}),
		});
		expect(isRecoveryFrames([f("admitted"), f("error", "SENDER_LOST")])).toBe(true);
		expect(isRecoveryFrames([f("admitted"), f("error", "PERMISSION_LOST")])).toBe(true);
		expect(isRecoveryFrames([f("admitted"), f("error", "UPSTREAM_ERROR")])).toBe(false);
		expect(isRecoveryFrames([f("admitted"), f("text"), f("done")])).toBe(false);
		expect(isRecoveryFrames([])).toBe(false);
	});

	it("decorates an original frame with its stream, a recovery frame with its origin only, and leaves a record without a stream as it was", () => {
		const frame: StreamFrame = { callId: "c", sequence: "2", type: "text", text: "hi" };
		const stream = newOriginalStreamId();
		expect(stream).toMatch(/^str_[0-9a-f]{24}$/);
		expect(withIdentity(frame, "original", stream)).toEqual({
			...frame,
			frameOrigin: "original",
			originalStreamId: stream,
		});
		expect(withIdentity(frame, "recovery", stream)).toEqual({ ...frame, frameOrigin: "recovery" });
		expect(withIdentity(frame, "original", undefined)).toBe(frame);
		expect(frame).not.toHaveProperty("frameOrigin");
	});

	it("text frames reserve the identity's bytes: a decorated frame stays within the route's frame bound", () => {
		const bounds = { maxFrameBytes: 160, maxOutputBytes: 100_000, maxFrames: 1000 };
		const frames = new Frames("call_identity_bound", bounds);
		frames.admitted();
		frames.text("é".repeat(400));
		const stream = newOriginalStreamId();
		for (const f of frames.frames) {
			expect(encodedFrameBytes(withIdentity(f, "original", stream))).toBeLessThanOrEqual(bounds.maxFrameBytes);
		}
		expect(frames.frames.length).toBeGreaterThan(2);
		expect(frameIdentityBytes).toBe(
			encodedFrameBytes(withIdentity(frames.frames[1] as StreamFrame, "original", stream)) -
				encodedFrameBytes(frames.frames[1] as StreamFrame),
		);
	});
});
