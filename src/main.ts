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
import { IdentityWatcher } from "./identity.js";
import { jsonLogger } from "./log.js";
import { Telemetry } from "./telemetry.js";
import {
	createHealthServer,
	createServer,
	DevelopmentIdentity,
	type Identity,
	MtlsIdentity,
} from "./transport/http.js";

export async function run(cfg: Config): Promise<void> {
	const log = jsonLogger();
	// A defect that escapes every guard is logged and the process goes on
	// (P1.3, B-28/B-29): an exit would cut every in-flight paid send of the
	// replica and reclaim each as unknown. The affected call, if any, ends
	// through its own paths (bounds, deadline, the sweep).
	process.on("uncaughtException", (err) => {
		log.error("uncaught exception; the process continues", {
			error: err instanceof Error ? `${err.name}: ${err.message}` : String(err),
		});
	});
	process.on("unhandledRejection", (reason) => {
		log.error("unhandled rejection; the process continues", {
			error: reason instanceof Error ? `${reason.name}: ${reason.message}` : String(reason),
		});
	});
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
	// One watched workload identity per process (P0.1): the mTLS listener
	// and the Control client share it when they name the same files.
	const id = cfg.identity.mtls;
	const identityWatcher =
		cfg.identity.mode === "mtls"
			? new IdentityWatcher({ certFile: id.certFile, keyFile: id.keyFile, caFile: id.caFile }, 5000, log)
			: undefined;
	identityWatcher?.start();
	const c = cfg.control.identity.mtls;
	const sameFiles = identityWatcher && c.certFile === id.certFile && c.keyFile === id.keyFile && c.caFile === id.caFile;
	const control = new ControlClient(
		cfg.control.address,
		cfg.control.timeoutMs,
		cfg.control.identity,
		sameFiles ? identityWatcher : undefined,
	);
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
	const telemetry = new Telemetry(cfg.telemetry, "anvilkit-agent-model-proxy");
	const server = createServer({ cfg, contract, calls, identity, identityWatcher, log, observer: telemetry });
	server.requestTimeout = 0;
	server.headersTimeout = cfg.http.requestHeaderTimeoutMs;
	const health = createHealthServer(
		() => ready,
		telemetry.registry,
		() => {
			ready = false;
		},
	);
	// The probe listener is up first: the kubelet's startup probe finds the
	// process, /readyz answers 503 until the service is serving.
	await listen(health, cfg.health.listen);
	await listen(server, cfg.http.listen);
	// The sweep is off the readiness path (P1.3, MP-07): the first pass runs
	// beside serving, and every pass is bounded (observation.sweep_batch).
	const runSweep = () =>
		calls.sweep().then(
			(r) =>
				log.info("sweep pass", {
					resubmitted: r.resubmitted,
					reclaimed: r.reclaimed,
					completed: r.completed,
					expired: r.expired,
				}),
			(err) => log.error("sweep failed", { error: String(err) }),
		);
	void runSweep();
	const sweep = setInterval(() => void runSweep(), cfg.observation.sweepIntervalMs);
	ready = true;
	log.info("model proxy serving", {
		listen: cfg.http.listen,
		healthListen: cfg.health.listen,
		identity: identity.describe(),
		store: objects.describe(),
		control: cfg.control.address,
		instanceId,
	});
	await new Promise<void>((resolve) => {
		const stop = () => {
			// The drain (P1.3, MP-05): readiness withdrawn, new opens refused as
			// retryable, the listener closed to new connections, in-flight sends
			// run to their bounded end and are settled and observed; the bound
			// covers the longest route send (validated by the configuration).
			ready = false;
			calls.beginClose();
			clearInterval(sweep);
			log.info("shutdown requested", { inFlight: calls.inFlight, boundMs: cfg.http.shutdownTimeoutMs });
			server.close();
			const bound = setTimeout(() => {
				log.error("drain bound reached with sends in flight; they are reclaimed by the sweep", {
					inFlight: calls.inFlight,
				});
				resolve();
			}, cfg.http.shutdownTimeoutMs);
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
	identityWatcher?.stop();
	await telemetry.shutdown().catch((err) => log.warn("span flush failed", { error: String(err) }));
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
