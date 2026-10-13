// The unpaid route health probe (P2.3): GET <base_url>/models with the
// route's credential, through node:https directly (the global fetch is
// refused in this process), no redirect followed, no body kept. A route
// whose latest probe failed refuses new calls as retryable
// DEPENDENCY_UNAVAILABLE; calls already admitted are never touched. No
// model is invoked: the probe proves reachability and the credential's
// acceptance, never a paid send.
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import type { Config, Route } from "../config.js";
import type { Logger } from "../log.js";

export interface ProbeResult {
	ok: boolean;
	status?: number;
	reason?: string;
}

/** One probe of a route's models listing. */
export function probeModels(route: Route, credential: string, timeoutMs: number): Promise<ProbeResult> {
	const base = new URL(route.baseUrl);
	const target = new URL(`${base.pathname.replace(/\/$/, "")}/models`, base.origin);
	const send = target.protocol === "https:" ? httpsRequest : httpRequest;
	return new Promise((resolve) => {
		const req = send(
			target,
			{
				method: "GET",
				headers: { authorization: `Bearer ${credential}`, accept: "application/json" },
				timeout: timeoutMs,
			},
			(res) => {
				res.resume();
				const status = res.statusCode ?? 0;
				resolve(
					status >= 200 && status < 300 ? { ok: true, status } : { ok: false, status, reason: `answered ${status}` },
				);
			},
		);
		req.on("timeout", () => req.destroy(new Error("timeout")));
		req.on("error", (err) => resolve({ ok: false, reason: err.message === "timeout" ? "timed out" : "unreachable" }));
		req.end();
	});
}

/** The latest probe of every served route that declares one. */
export class RouteHealth {
	private readonly state = new Map<string, ProbeResult>();
	private timer: NodeJS.Timeout | undefined;

	constructor(
		private readonly cfg: Config,
		private readonly routes: Route[],
		private readonly log: Logger,
		private readonly intervalMs = 60_000,
		private readonly timeoutMs = 5_000,
	) {}

	/** Routes without a probe are always healthy. */
	healthy(routeId: string): boolean {
		return this.state.get(routeId)?.ok ?? !this.routes.some((r) => r.id === routeId);
	}

	async probeAll(): Promise<void> {
		await Promise.all(
			this.routes.map(async (r) => {
				const credential = this.cfg.credentials.get(r.id);
				const result = credential
					? await probeModels(r, credential, this.timeoutMs)
					: { ok: false, reason: "no credential" };
				const previous = this.state.get(r.id);
				this.state.set(r.id, result);
				if (!previous || previous.ok !== result.ok)
					(result.ok ? this.log.info : this.log.warn).call(this.log, "route health probe", {
						routeId: r.id,
						healthy: result.ok,
						status: result.status,
						reason: result.reason,
					});
			}),
		);
	}

	start(): Promise<void> {
		if (this.routes.length === 0) return Promise.resolve();
		this.timer = setInterval(() => void this.probeAll(), this.intervalMs);
		this.timer.unref();
		return this.probeAll();
	}

	stop(): void {
		if (this.timer) clearInterval(this.timer);
	}
}
