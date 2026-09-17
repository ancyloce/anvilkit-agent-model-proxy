import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ControlClient, ControlRefused, ControlUnavailable } from "../src/adapters/control.js";
import { FakeControl } from "./control.js";

const digest = "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3";

describe("Control dispatch adapter", () => {
	let control: FakeControl;
	let client: ControlClient;
	beforeAll(async () => {
		control = await new FakeControl().start();
		client = new ControlClient(control.address, 2_000, {
			mode: "development",
			mtls: { certFile: "", keyFile: "", caFile: "", serverName: "" },
		});
	});
	afterAll(async () => {
		client.close();
		await control.stop();
	});

	const admit = (callId: string, overrides: Record<string, unknown> = {}) =>
		client.admitModel({
			tenantId: "tenant_a",
			actorId: "anvilkit-agent-workflow",
			commandId: `admit-${callId}`,
			requestDigest: digest,
			binding: { tenantId: "tenant_a", operationId: "op_1", attemptId: "att_1", executionEpoch: "1" },
			callId,
			owner: "anvilkit-agent-model-proxy",
			routeId: "controlled-openai-v1",
			provider: "fixture",
			model: "fixture-model",
			maxExposure: { currency: "USD", amount: "1000" },
			deadline: new Date(Date.now() + 60_000),
			...overrides,
		});

	it("is told dispatchAllowed=true exactly once per call and reads the recorded decision afterwards", async () => {
		const first = await admit("call_a");
		expect(first).toMatchObject({ allowed: true, state: "authorized" });
		const again = await admit("call_a");
		expect(again).toMatchObject({ allowed: false, state: "authorized", dispatchId: first.dispatchId });
		expect(again.denialCode).toBeUndefined();
		await expect(
			admit("call_a", { requestDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111" }),
		).rejects.toMatchObject({ code: "IDEMPOTENCY_CONFLICT" });
		control.denials.set("denied-route", "PROFILE_UNQUALIFIED");
		const denied = await admit("call_b", { routeId: "denied-route" });
		expect(denied).toMatchObject({ allowed: false, state: "denied", denialCode: "PROFILE_UNQUALIFIED" });
		expect(await client.getDispatch(first.dispatchId)).toMatchObject({
			callId: "call_a",
			owner: "anvilkit-agent-model-proxy",
			state: "authorized",
		});
	});

	it("keeps an observation without usage apart from explicit zero usage and deduplicates by source and sequence", async () => {
		const a = await admit("call_c");
		await expect(
			client.observe({
				dispatchId: a.dispatchId,
				source: "model-proxy/x",
				sequence: "1",
				outcome: "succeeded",
				observedAt: new Date(),
			}),
		).rejects.toMatchObject({ code: "EFFECT_UNCERTAIN" });
		const unknown = await client.observe({
			dispatchId: a.dispatchId,
			source: "model-proxy/x",
			sequence: "1",
			outcome: "unknown",
			observedAt: new Date(),
		});
		expect(unknown).toEqual({ existing: false, state: "unknown" });
		const dup = await client.observe({
			dispatchId: a.dispatchId,
			source: "model-proxy/x",
			sequence: "1",
			outcome: "unknown",
			observedAt: new Date(),
		});
		expect(dup.existing).toBe(true);
		const zero = await client.observe({
			dispatchId: a.dispatchId,
			source: "model-proxy/x",
			sequence: "2",
			outcome: "succeeded",
			usage: { inputUnits: "0", outputUnits: "0", reasoningUnits: "0", cachedInputUnits: "0" },
			observedAt: new Date(),
		});
		expect(zero).toEqual({ existing: false, state: "observed" });
		const sent = control.observes.at(-1);
		expect(sent?.cumulativeUsage).toMatchObject({ inputUnits: "0" });
		expect(control.observes.at(-3)?.cumulativeUsage).toBeUndefined();
	});

	it("validates every request before it is sent and maps unavailability apart from refusals", async () => {
		const before = control.admits.length;
		await expect(admit("call_d", { requestDigest: "not-a-digest" })).rejects.toMatchObject({
			code: "INVALID_ARGUMENT",
		});
		await expect(admit("call_d", { maxExposure: { currency: "usd", amount: "1" } })).rejects.toBeInstanceOf(
			ControlRefused,
		);
		expect(control.admits.length).toBe(before);
		control.unavailable = 1;
		await expect(admit("call_e")).rejects.toBeInstanceOf(ControlUnavailable);
		await expect(client.getDispatch("dsp_missing")).rejects.toMatchObject({ code: "NOT_FOUND" });
		const unreachable = new ControlClient("127.0.0.1:1", 500, {
			mode: "development",
			mtls: { certFile: "", keyFile: "", caFile: "", serverName: "" },
		});
		await expect(unreachable.getDispatch("dsp_x")).rejects.toBeInstanceOf(ControlUnavailable);
		unreachable.close();
	});
});
