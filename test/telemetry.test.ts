import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace";
import { describe, expect, it } from "vitest";
import { routeOf, Telemetry } from "../src/telemetry.js";
import { checkedInConfig, configFrom } from "./helpers.js";

describe("telemetry", () => {
	it("names routes by template: a call identifier never becomes a route", () => {
		expect(routeOf("/api/v1/model-calls")).toBe("/api/v1/model-calls");
		expect(routeOf("/api/v1/model-calls/call-secret-1")).toBe("/api/v1/model-calls/{callId}");
		expect(routeOf("/api/v1/model-calls/call-secret-1/cancellations")).toBe(
			"/api/v1/model-calls/{callId}/cancellations",
		);
		expect(routeOf("/other/call-secret-1")).toBe("unmatched");
	});

	it("records a span and metrics with the method, route template and status only", async () => {
		const exporter = new InMemorySpanExporter();
		const t = new Telemetry({ otlpEndpoint: "", sampleRatio: 1 }, "test", new SimpleSpanProcessor({ exporter }));
		const done = t.request("POST", routeOf("/api/v1/model-calls/call-secret-1/cancellations"), {
			traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			authorization: "Bearer secret-token-value",
		});
		done(409);
		done(500); // a second end is ignored
		await new Promise((resolve) => setImmediate(resolve)); // the processor exports asynchronously
		const spans = exporter.getFinishedSpans();
		expect(spans).toHaveLength(1);
		const span = spans[0];
		expect(span?.name).toBe("POST /api/v1/model-calls/{callId}/cancellations");
		expect(span?.attributes).toEqual({
			"http.request.method": "POST",
			"http.route": "/api/v1/model-calls/{callId}/cancellations",
			"http.response.status_code": 409,
		});
		expect(span?.spanContext().traceId).toBe("0af7651916cd43dd8448eb211c80319c");
		const metrics = await t.registry.metrics();
		expect(metrics).toContain(
			'anvilkit_model_proxy_requests_total{method="POST",route="/api/v1/model-calls/{callId}/cancellations",code="409"} 1',
		);
		expect(metrics).not.toContain("call-secret-1");
		expect(metrics).not.toContain("secret-token-value");
		await t.shutdown();
	});

	it("is configured by placement and validated", () => {
		expect(configFrom(checkedInConfig()).telemetry).toEqual({ otlpEndpoint: "", sampleRatio: 1 });
		expect(() => configFrom(`${checkedInConfig()}\ntelemetry:\n  sample_ratio: 2\n`)).toThrow(/telemetry.sample_ratio/);
		expect(() => configFrom(`${checkedInConfig()}\ntelemetry:\n  otlp_endpoint: collector:4318\n`)).toThrow(
			/telemetry.otlp_endpoint/,
		);
	});

	it("counts call outcomes and sweep passes without identifiers (P3.6)", async () => {
		const t = new Telemetry({ otlpEndpoint: "", sampleRatio: 1 }, "test");
		t.outcome("succeeded");
		t.outcome("unknown", "SENDER_LOST");
		t.outcome("unknown", "PERMISSION_LOST");
		t.outcome("unknown", "SENDER_LOST");
		t.sweep({ resubmitted: 2, reclaimed: 1, completed: 0, expired: 3, pending: 4 }, 0.25);
		const metrics = await t.registry.metrics();
		expect(metrics).toContain('anvilkit_model_proxy_call_outcomes_total{outcome="succeeded",error_code="none"} 1');
		expect(metrics).toContain('anvilkit_model_proxy_call_outcomes_total{outcome="unknown",error_code="SENDER_LOST"} 2');
		expect(metrics).toContain(
			'anvilkit_model_proxy_call_outcomes_total{outcome="unknown",error_code="PERMISSION_LOST"} 1',
		);
		expect(metrics).toContain('anvilkit_model_proxy_sweep_entries_total{action="reclaimed"} 1');
		expect(metrics).toContain('anvilkit_model_proxy_sweep_entries_total{action="expired"} 3');
		expect(metrics).toContain("anvilkit_model_proxy_pending_observations 4");
		expect(metrics).toContain("anvilkit_model_proxy_sweep_duration_seconds_count 1");
		await t.shutdown();
	});

	it("hands the sampled server span's context to the request (P3.6)", async () => {
		const exporter = new InMemorySpanExporter();
		const t = new Telemetry({ otlpEndpoint: "", sampleRatio: 1 }, "test", new SimpleSpanProcessor({ exporter }));
		const done = t.request("POST", "/api/v1/model-calls", {
			traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		});
		expect(done.traceparent).toMatch(/^00-0af7651916cd43dd8448eb211c80319c-[0-9a-f]{16}-01$/);
		expect(done.traceparent).not.toContain("b7ad6b7169203331");
		done(200);
		await t.shutdown();
	});
});
