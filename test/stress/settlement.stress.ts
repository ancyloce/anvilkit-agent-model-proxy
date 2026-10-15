// P3.5 AC4: the stress case of the open IT-MODEL-PROXY intermittent (P22,
// run 20260930T231937Z: an invalid-usage call carried two usage observations
// where one was asserted). Two Proxy instances on one store; the sender is
// instance A, instance B sweeps in a tight loop while A's calls settle.
//   1. Under the real clocks, N iterations (default 100) of an invalid-usage
//      call each count the calls that end with more than one observation.
//      Root cause (closed): FilesystemStore's conditional put was check-then-
//      act, so A's settlement and B's sweep completing it from A's evidence
//      both passed the version check at one version and both published one
//      observation each ({model-proxy/a, 2} and {model-proxy/b, 2}); with
//      valid usage that would be a double report. The put now holds the key's
//      lock file across the check and the rename (0/100 since; 7/100 and
//      8/40 before).
//   2. The forced interleaving: B's clock runs past the call's deadline plus
//      the reclaim grace while A is still sending, so B reclaims the live send
//      as unknown/SENDER_LOST before A settles. A's later settlement replaces
//      the reclaimed unknown only with more evidence: without usage the
//      reclaim's observation stays the only one; with usage A adds one
//      supplemental observation under a fresh sequence.
import { DispatchOutcome } from "@anvilkit/generated-clients/proto/anvilkit/control/v1/dispatch";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { FakeControl } from "../control.js";
import { call, type RunningProxy, requestBody, startProxy } from "../proxy.js";
import { Upstream } from "../upstream.js";

const iterations = Number(process.env.ANVILKIT_MODEL_PROXY_STRESS_ITERATIONS ?? "100");
const invalidUsage = { prompt_tokens: -1, completion_tokens: 2 };

describe("reclaim versus settlement (P3.5 stress)", () => {
	let up: Upstream;
	let control: FakeControl;
	let a: RunningProxy;
	let b: RunningProxy;
	let skewMs = 0;
	beforeAll(async () => {
		up = await new Upstream().start();
		control = await new FakeControl().start();
		a = await startProxy({ upstreamUrl: up.url, controlAddress: control.address, instanceId: "a" });
		b = await startProxy({
			upstreamUrl: up.url,
			controlAddress: control.address,
			instanceId: "b",
			storeDir: a.storeDir,
			now: () => Date.now() + skewMs,
		});
	});
	afterAll(async () => {
		await a.close();
		await b.close();
		await control.stop();
		await up.stop();
	});

	const sweepWhile = async (done: () => boolean) => {
		while (!done()) {
			await b.calls.sweep();
			await new Promise((r) => setImmediate(r));
		}
	};

	it(`under the real clocks, ${iterations} invalid-usage calls settle with one send each while the other instance sweeps`, async () => {
		const doubled: string[] = [];
		for (let i = 0; i < iterations; i++) {
			const callId = `call_stress_${Date.now()}_${i}`;
			up.next({ kind: "stream", text: ["s"], usage: invalidUsage });
			const before = up.receives.length;
			let finished = false;
			const sweeping = sweepWhile(() => finished);
			const r = await call(a.url, "POST", "/api/v1/model-calls", requestBody(callId));
			finished = true;
			await sweeping;
			expect(r.status, r.text).toBe(200);
			expect(r.frames.at(-1)).toMatchObject({ type: "error", outcome: "unknown" });
			expect(up.receives.length - before, "one physical send per call").toBe(1);
			const recorded = control.byCall(callId);
			expect(recorded?.observations.every((o) => o.outcome === recorded.observations[0]?.outcome)).toBe(true);
			if ((recorded?.observations.length ?? 0) > 1) {
				doubled.push(callId);
				if (doubled.length <= 2)
					console.log(
						JSON.stringify({
							callId,
							observations: recorded?.observations.map((o) => ({
								source: o.source,
								sequence: o.sequence,
								outcome: o.outcome,
								usage: o.usage ?? null,
							})),
						}),
					);
			}
		}
		// Recorded for the run: how often the race happened under these clocks.
		console.log(JSON.stringify({ iterations, callsWithTwoObservations: doubled.length }));
		expect(doubled, "no reclaim races a sender that settles before its deadline").toEqual([]);
	});

	const forced = async (usage: { prompt_tokens: number; completion_tokens: number }, want: number) => {
		const callId = `call_stress_forced_${Date.now()}_${want}`;
		up.next({ kind: "stream-then-hang", text: ["s"], usageBeforeHang: usage });
		const before = up.receives.length;
		const sending = call(a.url, "POST", "/api/v1/model-calls", requestBody(callId));
		await up.awaitHeld();
		// B's clock is past the deadline plus the grace: the live send looks lost.
		skewMs = 24 * 60 * 60 * 1000;
		const reclaimed = await b.calls.sweep();
		skewMs = 0;
		expect(reclaimed.reclaimed).toBeGreaterThanOrEqual(1);
		up.release();
		const r = await sending;
		expect(r.status).toBe(200);
		expect(up.receives.length - before, "never resent").toBe(1);
		// Let A's settlement and any retried observation land.
		const deadline = Date.now() + 3_000;
		while (Date.now() < deadline) {
			await a.calls.sweep();
			await new Promise((res) => setTimeout(res, 50));
		}
		const recorded = control.byCall(callId);
		const observations = recorded?.observations ?? [];
		expect(observations.length).toBe(want);
		expect(observations[0]).toMatchObject({
			source: "model-proxy/b",
			outcome: DispatchOutcome.DISPATCH_OUTCOME_UNKNOWN,
		});
		expect(observations[0]?.usage, "the reclaim never carries usage").toBeUndefined();
		expect(
			new Set(observations.map((o) => `${o.source}/${o.sequence}`)).size,
			"every observation under its own sequence",
		).toBe(want);
		return observations;
	};

	it("a reclaim that precedes a settlement without usage stays the only observation: one send, unknown, nothing charged", async () => {
		await forced(invalidUsage, 1);
	});

	it("a reclaim that precedes a settlement with usage gets one supplemental observation carrying the usage", async () => {
		const observations = await forced({ prompt_tokens: 3, completion_tokens: 2 }, 2);
		expect(observations[1]?.usage, "the sender's evidence settles the usage once").toBeDefined();
	});
});
