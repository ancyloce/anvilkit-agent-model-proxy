// The Proxy's redacted signals (security.md "data classification, logging
// and deletion"): one server span and the request metrics per business
// request, with controlled attributes only - the method, the route template
// and the status. A call identifier, a body, a prompt, a header value, a
// token or a native model response never becomes a span attribute or a
// metric label. Spans go over OTLP/HTTP to the collector when an endpoint is
// placed and nowhere otherwise; the metrics are served on the probe listener.
import type { IncomingHttpHeaders } from "node:http";
import { defaultTextMapGetter, ROOT_CONTEXT, SpanKind, SpanStatusCode, type Tracer, trace } from "@opentelemetry/api";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
	BatchSpanProcessor,
	ParentBasedSampler,
	type SpanProcessor,
	TraceIdRatioBasedSampler,
	TracerProvider,
} from "@opentelemetry/sdk-trace";
import { Counter, collectDefaultMetrics, Histogram, Registry } from "prom-client";

export interface TelemetryConfig {
	otlpEndpoint: string;
	sampleRatio: number;
}

/** Observes one request: returns the function that records its end. */
export interface RequestObserver {
	request(method: string, route: string, headers: IncomingHttpHeaders): (status: number) => void;
}

const callRoute = /^\/api\/v1\/model-calls\/[^/]+(\/cancellations)?$/;

/** The route template of a business path; a call identifier never appears in it. */
export function routeOf(pathname: string): string {
	if (pathname === "/api/v1/model-calls") return pathname;
	const m = callRoute.exec(pathname);
	if (m) return `/api/v1/model-calls/{callId}${m[1] ?? ""}`;
	return "unmatched";
}

export class Telemetry implements RequestObserver {
	readonly registry = new Registry();
	private readonly tracer: Tracer;
	private readonly provider: TracerProvider | undefined;
	private readonly propagator = new W3CTraceContextPropagator();
	private readonly requests: Counter<"method" | "route" | "code">;
	private readonly duration: Histogram<"method" | "route">;

	constructor(cfg: TelemetryConfig, service: string, processor?: SpanProcessor) {
		collectDefaultMetrics({ register: this.registry });
		this.requests = new Counter({
			name: "anvilkit_model_proxy_requests_total",
			help: "Business requests by method, route template and status.",
			labelNames: ["method", "route", "code"],
			registers: [this.registry],
		});
		this.duration = new Histogram({
			name: "anvilkit_model_proxy_request_duration_seconds",
			help: "Business request duration (streams included) by method and route template.",
			labelNames: ["method", "route"],
			buckets: [0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60, 300, 900],
			registers: [this.registry],
		});
		const spans =
			processor ??
			(cfg.otlpEndpoint
				? new BatchSpanProcessor({
						exporter: new OTLPTraceExporter({ url: `${cfg.otlpEndpoint.replace(/\/$/, "")}/v1/traces` }),
					})
				: undefined);
		if (spans) {
			this.provider = new TracerProvider({
				resource: resourceFromAttributes({ "service.name": service }),
				sampler: new ParentBasedSampler({ root: new TraceIdRatioBasedSampler(cfg.sampleRatio) }),
				spanProcessors: [spans],
			});
			this.tracer = this.provider.getTracer(service);
		} else {
			this.tracer = trace.getTracer(service);
		}
	}

	request(method: string, route: string, headers: IncomingHttpHeaders): (status: number) => void {
		const start = process.hrtime.bigint();
		// The callers are the Workflow and the access sidecar: their trace
		// context is continued, nothing else of the request is read.
		const parent = this.propagator.extract(ROOT_CONTEXT, { traceparent: headers.traceparent }, defaultTextMapGetter);
		const span = this.tracer.startSpan(
			`${method} ${route}`,
			{ kind: SpanKind.SERVER, attributes: { "http.request.method": method, "http.route": route } },
			parent,
		);
		let ended = false;
		return (status: number) => {
			if (ended) return;
			ended = true;
			span.setAttribute("http.response.status_code", status);
			if (status >= 500) span.setStatus({ code: SpanStatusCode.ERROR });
			span.end();
			this.requests.inc({ method, route, code: String(status) });
			this.duration.observe({ method, route }, Number(process.hrtime.bigint() - start) / 1e9);
		};
	}

	/** Flushes the spans within the bound; the provider is closed afterwards. */
	async shutdown(): Promise<void> {
		await this.provider?.shutdown();
	}
}
