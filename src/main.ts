// Bootstrap of anvilkit-agent-model-proxy: configuration, the refusal of
// any process-wide fetch (every send goes through a call's ticket), the
// store and its qualification, the Control client, the call service with
// its sweep, the business listener and the probe listener; SIGTERM drains
// within the shutdown bound while in-flight sends run to their bounded end
// so their usage is observed.

import { randomBytes } from "node:crypto";
import { hostname } from "node:os";
import { ControlClient } from "./adapters/control.js";
import { CallStore, newObjectStore } from "./adapters/store.js";
import { refuseGlobalFetch } from "./adapters/transport.js";
import { CallService } from "./application/calls.js";
import { type Config, disabled, load, routeDisabledReasons } from "./config.js";
import { Contract } from "./contracts.js";
import { jsonLogger } from "./log.js";
import {
	createHealthServer,
	createServer,
	DevelopmentIdentity,
	type Identity,
	MtlsIdentity,
} from "./transport/http.js";

export async function run(cfg: Config): Promise<void> {
	const log = jsonLogger();
	if (disabled(cfg)) {
		throw new Error(
			"profile disabled: identity.mode is disabled (no trusted identity input for this environment); nothing is served",
		);
	}
	refuseGlobalFetch();
	const contract = new Contract(cfg.contractsDir || undefined);
	const objects = newObjectStore(cfg);
	if (cfg.store.backend !== "s3" || cfg.store.s3.qualifyOnStart) await objects.qualify();
	const store = new CallStore(objects);
	const control = new ControlClient(cfg.control.address, cfg.control.timeoutMs, cfg.control.identity);
	const instanceId = `${hostname().slice(0, 24)}-${randomBytes(4).toString("hex")}`;
	const calls = new CallService({ cfg, contract, control, store, log, instanceId });
	const identity: Identity =
		cfg.identity.mode === "mtls"
			? new MtlsIdentity(cfg.identity.mtls.principals)
			: new DevelopmentIdentity(cfg.identity.principalsFile);
	if (cfg.identity.mode === "development")
		log.warn("DEVELOPMENT_ONLY identity: bearer principals; qualifies no production identity");
	if (cfg.control.identity.mode === "development") log.warn("DEVELOPMENT_ONLY Control transport: plaintext gRPC");
	for (const r of cfg.routes) {
		const reasons = routeDisabledReasons(cfg, r);
		if (reasons.length > 0) log.warn("route not served", { routeId: r.id, reasons: reasons.join("; ") });
		else
			log.info("route served", {
				routeId: r.id,
				api: r.api,
				provider: r.provider,
				model: r.model,
				origin: new URL(r.baseUrl).origin,
			});
	}
	let ready = false;
	const server = createServer({ cfg, contract, calls, identity, log });
	server.requestTimeout = 0;
	server.headersTimeout = cfg.http.requestHeaderTimeoutMs;
	const health = createHealthServer(() => ready);
	// The probe listener is up first: the kubelet's startup probe finds the
	// process, /readyz answers 503 until the service is serving.
	await listen(health, cfg.health.listen);
	await listen(server, cfg.http.listen);
	const first = await calls.sweep();
	const sweep = setInterval(
		() => void calls.sweep().catch((err) => log.error("sweep failed", { error: String(err) })),
		cfg.observation.sweepIntervalMs,
	);
	ready = true;
	log.info("model proxy serving", {
		listen: cfg.http.listen,
		healthListen: cfg.health.listen,
		identity: identity.describe(),
		store: objects.describe(),
		control: cfg.control.address,
		instanceId,
		resubmitted: first.resubmitted,
		reclaimed: first.reclaimed,
		completed: first.completed,
	});
	await new Promise<void>((resolve) => {
		const stop = () => {
			ready = false;
			clearInterval(sweep);
			log.info("shutdown requested", { inFlight: calls.inFlight });
			server.close();
			const bound = setTimeout(() => resolve(), cfg.http.shutdownTimeoutMs);
			void calls.close().then(() => {
				clearTimeout(bound);
				resolve();
			});
		};
		process.once("SIGTERM", stop);
		process.once("SIGINT", stop);
	});
	health.close();
	control.close();
}

function listen(server: import("node:http").Server, address: string): Promise<void> {
	const i = address.lastIndexOf(":");
	const host = address.slice(0, i);
	const port = Number(address.slice(i + 1));
	return new Promise<void>((resolve, reject) => {
		server.once("error", reject);
		server.listen(port, host, () => resolve());
	});
}

const entry = process.argv[1] ?? "";
if (entry.endsWith("main.js") || entry.endsWith("main.ts")) {
	run(load())
		.then(() => process.exit(0))
		.catch((err) => {
			process.stderr.write(`${err instanceof Error ? err.message : String(err)}\n`);
			process.exit(err instanceof Error && /profile disabled/.test(err.message) ? 3 : 1);
		});
}
