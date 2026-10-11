// The Proxy's grpc-js client of anvilkit.control.v1.DispatchService (DD-02
// §4): the single-use model admission, the idempotent outcome/usage
// observation and the original-identity query. Every request passes the
// contract's explicit TypeScript validation (protovalidate over the
// generated descriptors) before it is sent; Control's public refusal code
// is carried in front of the status message, as the Go callers read it.
import {
	AdmitModelRequest,
	DispatchOutcome,
	DispatchServiceClient,
	DispatchState,
	GetDispatchRequest,
	ObserveDispatchRequest,
} from "@anvilkit/generated-clients/proto/anvilkit/control/v1/dispatch";
import { validateJson } from "@anvilkit/generated-clients/validation/rpc";
import {
	type ChannelCredentials,
	type ChannelOptions,
	credentials,
	Metadata,
	type ServiceError,
	status,
} from "@grpc/grpc-js";
import type { Config } from "../config.js";
import type { ExecutionBinding, Outcome, Usage } from "../contracts.js";
import { clientCredentials, clientOptions, IdentityWatcher } from "../identity.js";

/** Control refused the request on a precondition; code is the public error code. */
export class ControlRefused extends Error {
	constructor(
		readonly code: string,
		message: string,
	) {
		super(message);
	}
}

/** Control could not be asked or did not answer; nothing is established. */
export class ControlUnavailable extends Error {}

export interface AdmissionResult {
	dispatchId: string;
	state: "prepared" | "authorized" | "observed" | "unknown" | "confirmed_not_sent" | "denied";
	allowed: boolean;
	denialCode?: string;
}

export interface AdmitInput {
	tenantId: string;
	actorId: string;
	commandId: string;
	requestDigest: string;
	binding: ExecutionBinding;
	callId: string;
	owner: string;
	routeId: string;
	provider: string;
	model: string;
	maxExposure: { currency: string; amount: string };
	deadline: Date;
	supersedesCallId?: string;
	evidenceRef?: string;
}

export interface ObserveInput {
	dispatchId: string;
	source: string;
	sequence: string;
	outcome: Outcome;
	usage?: Usage;
	nativeReference?: string;
	observedAt: Date;
}

export interface DispatchView {
	dispatchId: string;
	callId: string;
	owner: string;
	state: AdmissionResult["state"];
	outcome?: Outcome;
}

export interface DispatchPort {
	admitModel(input: AdmitInput): Promise<AdmissionResult>;
	observe(input: ObserveInput): Promise<{ existing: boolean; state: AdmissionResult["state"] }>;
	getDispatch(tenantId: string, dispatchId: string): Promise<DispatchView>;
	close(): void;
}

const stateNames: Record<DispatchState, AdmissionResult["state"] | undefined> = {
	[DispatchState.DISPATCH_STATE_UNSPECIFIED]: undefined,
	[DispatchState.DISPATCH_STATE_PREPARED]: "prepared",
	[DispatchState.DISPATCH_STATE_AUTHORIZED]: "authorized",
	[DispatchState.DISPATCH_STATE_OBSERVED]: "observed",
	[DispatchState.DISPATCH_STATE_UNKNOWN]: "unknown",
	[DispatchState.DISPATCH_STATE_CONFIRMED_NOT_SENT]: "confirmed_not_sent",
	[DispatchState.DISPATCH_STATE_DENIED]: "denied",
	[DispatchState.UNRECOGNIZED]: undefined,
};

const outcomeToProto: Record<Outcome, DispatchOutcome> = {
	succeeded: DispatchOutcome.DISPATCH_OUTCOME_SUCCEEDED,
	failed: DispatchOutcome.DISPATCH_OUTCOME_FAILED,
	canceled: DispatchOutcome.DISPATCH_OUTCOME_CANCELED,
	unknown: DispatchOutcome.DISPATCH_OUTCOME_UNKNOWN,
};

const outcomeFromProto: Partial<Record<DispatchOutcome, Outcome>> = {
	[DispatchOutcome.DISPATCH_OUTCOME_SUCCEEDED]: "succeeded",
	[DispatchOutcome.DISPATCH_OUTCOME_FAILED]: "failed",
	[DispatchOutcome.DISPATCH_OUTCOME_CANCELED]: "canceled",
	[DispatchOutcome.DISPATCH_OUTCOME_UNKNOWN]: "unknown",
};

function stateOf(s: DispatchState | undefined): AdmissionResult["state"] {
	const name = s === undefined ? undefined : stateNames[s];
	if (!name) throw new ControlUnavailable(`Control answered a dispatch without a known state (${s})`);
	return name;
}

/** Control's pool is full: nothing was decided; the caller retries later (P1.3, MP-04). */
export class ControlCapacityExhausted extends ControlUnavailable {}

type Phase = "admit" | "observe" | "query";

/**
 * Maps a gRPC status to the Proxy's errors by what the phase can conclude
 * from it (P1.3, review finding B-30). CAPACITY_EXHAUSTED decides nothing
 * and is always retryable. On an admission a precondition refusal (stale
 * execution, budget, profile, conflicting digest) is Control's decision on
 * the call. On an observation only an answer about the dispatch identity
 * itself — no such dispatch, not this owner's, a malformed report — is
 * terminal; a transient refusal (resource exhaustion, a revision conflict,
 * a failed precondition of the moment, unavailability, a deadline) keeps the
 * observation pending under its identity and the sweep resubmits it: usage
 * is never discarded on a transient answer.
 */
function refusal(err: ServiceError, phase: Phase): Error {
	const code = (err.details || "").split(":")[0]?.trim() || status[err.code] || "REFUSED";
	if (code === "CAPACITY_EXHAUSTED")
		return new ControlCapacityExhausted(`control CAPACITY_EXHAUSTED: ${err.details || err.message}`);
	// Two precondition answers are permanent facts about the dispatch itself,
	// whatever the moment: a report without usage for a definite outcome
	// (EFFECT_UNCERTAIN) and a dispatch that never held a permission
	// (STALE_EXECUTION); repeating them changes nothing.
	const permanentPrecondition =
		err.code === status.FAILED_PRECONDITION && (code === "EFFECT_UNCERTAIN" || code === "STALE_EXECUTION");
	const terminal =
		phase === "observe"
			? permanentPrecondition
				? [status.FAILED_PRECONDITION, status.NOT_FOUND, status.PERMISSION_DENIED, status.INVALID_ARGUMENT]
				: [status.NOT_FOUND, status.PERMISSION_DENIED, status.INVALID_ARGUMENT]
			: [
					status.FAILED_PRECONDITION,
					status.ABORTED,
					status.INVALID_ARGUMENT,
					status.NOT_FOUND,
					status.PERMISSION_DENIED,
					status.RESOURCE_EXHAUSTED,
					status.DATA_LOSS,
				];
	if (terminal.includes(err.code)) return new ControlRefused(code, err.details || err.message);
	return new ControlUnavailable(`control ${status[err.code] ?? err.code}: ${err.details || err.message}`);
}

/**
 * The Control transport under control.identity (P0.1): mtls presents the
 * watched workload certificate and verifies Control through standard
 * hostname verification against mtls.server_name (a DNS SAN of Control's
 * certificate; never a CN comparison); development is the plaintext gRPC
 * of the development foundation, which only the configuration loader
 * admits under development.enabled.
 */
export interface ControlTransport {
	creds: ChannelCredentials;
	options: ChannelOptions;
	close(): void;
}

export function controlTransport(
	cfg: Config["control"]["identity"],
	watcher?: IdentityWatcher,
	log: { warn(msg: string, f?: Record<string, string>): void } = { warn: () => {} },
): ControlTransport {
	if (cfg.mode === "mtls") {
		const m = cfg.mtls;
		const own =
			watcher ?? new IdentityWatcher({ certFile: m.certFile, keyFile: m.keyFile, caFile: m.caFile }, 5000, log);
		if (!watcher) own.start();
		return {
			creds: clientCredentials(own),
			options: clientOptions(m.serverName),
			close: () => {
				if (!watcher) own.stop();
			},
		};
	}
	log.warn("DEVELOPMENT_ONLY Control transport: plaintext gRPC; qualifies no production identity");
	return { creds: credentials.createInsecure(), options: {}, close: () => {} };
}

/** Validates a ts-proto message through the contract's protovalidate boundary before it is sent. */
function validated(typeName: string, json: unknown): void {
	const v = validateJson(typeName, JSON.stringify(json));
	if (!v.valid) {
		const detail = v.reason === "malformed" ? v.error.message : v.violations.map((x) => x.toString()).join("; ");
		throw new ControlRefused("INVALID_ARGUMENT", `${typeName}: ${detail}`);
	}
}

export class ControlClient implements DispatchPort {
	private readonly client: DispatchServiceClient;
	private readonly transport: ControlTransport;

	constructor(
		address: string,
		private readonly timeoutMs: number,
		identity: Config["control"]["identity"],
		watcher?: IdentityWatcher,
	) {
		this.transport = controlTransport(identity, watcher);
		this.client = new DispatchServiceClient(address, this.transport.creds, {
			...this.transport.options,
			// The Proxy never lets the channel retry a unary call by itself: an
			// admission asked twice is the same durable command, and a lost
			// answer is reentered by the caller under the same identity.
			"grpc.enable_retries": 0,
		});
	}

	close(): void {
		this.client.close();
		this.transport.close();
	}

	private call<Req, Res>(
		fn: (req: Req, md: Metadata, opts: { deadline: Date }, cb: (err: ServiceError | null, res: Res) => void) => unknown,
		req: Req,
		phase: Phase,
	): Promise<Res> {
		return new Promise<Res>((resolve, reject) => {
			fn.call(this.client, req, new Metadata(), { deadline: new Date(Date.now() + this.timeoutMs) }, (err, res) => {
				if (err) reject(refusal(err, phase));
				else resolve(res);
			});
		});
	}

	async admitModel(input: AdmitInput): Promise<AdmissionResult> {
		const req = AdmitModelRequest.fromPartial({
			command: {
				tenantId: input.tenantId,
				commandId: input.commandId,
				actorId: input.actorId,
				requestDigest: input.requestDigest,
			},
			binding: {
				operationId: input.binding.operationId,
				attemptId: input.binding.attemptId,
				instanceId: input.binding.instanceId ?? "",
				executionEpoch: input.binding.executionEpoch,
			},
			callId: input.callId,
			owner: input.owner,
			routeId: input.routeId,
			provider: input.provider,
			model: input.model,
			maxExposure: { currency: input.maxExposure.currency, amount: input.maxExposure.amount },
			deadline: input.deadline,
			supersedesCallId: input.supersedesCallId,
			evidenceRef: input.evidenceRef,
		});
		validated("anvilkit.control.v1.AdmitModelRequest", AdmitModelRequest.toJSON(req));
		const res = await this.call(this.client.admitModel, req, "admit");
		const a = res.admission;
		if (!a?.dispatch) throw new ControlUnavailable("Control answered no admission");
		const out: AdmissionResult = {
			dispatchId: a.dispatch.dispatchId,
			state: stateOf(a.dispatch.state),
			allowed: a.dispatchAllowed === true,
		};
		if (a.denialCode) out.denialCode = a.denialCode;
		return out;
	}

	async observe(input: ObserveInput): Promise<{ existing: boolean; state: AdmissionResult["state"] }> {
		const req = ObserveDispatchRequest.fromPartial({
			dispatchId: input.dispatchId,
			source: input.source,
			sequence: input.sequence,
			outcome: outcomeToProto[input.outcome],
			// cumulativeUsage stays absent (not zero) when the sender has no counters.
			cumulativeUsage: input.usage
				? {
						inputUnits: input.usage.inputUnits,
						outputUnits: input.usage.outputUnits,
						reasoningUnits: input.usage.reasoningUnits,
						cachedInputUnits: input.usage.cachedInputUnits,
					}
				: undefined,
			nativeReference: input.nativeReference ?? "",
			observedAt: input.observedAt,
		});
		validated("anvilkit.control.v1.ObserveDispatchRequest", ObserveDispatchRequest.toJSON(req));
		const res = await this.call(this.client.observeDispatch, req, "observe");
		if (!res.dispatch) throw new ControlUnavailable("Control answered no dispatch");
		return { existing: res.existing, state: stateOf(res.dispatch.state) };
	}

	/** Reads one of this proxy's own dispatches of the tenant (P0.2: Control binds the read to the caller's owner identity and the tenant). */
	async getDispatch(tenantId: string, dispatchId: string): Promise<DispatchView> {
		const req = GetDispatchRequest.fromPartial({ dispatchId, tenantId });
		validated("anvilkit.control.v1.GetDispatchRequest", GetDispatchRequest.toJSON(req));
		const res = await this.call(this.client.getDispatch, req, "query");
		const d = res.dispatch;
		if (!d) throw new ControlUnavailable("Control answered no dispatch");
		const view: DispatchView = { dispatchId: d.dispatchId, callId: d.callId, owner: d.owner, state: stateOf(d.state) };
		const outcome = outcomeFromProto[d.outcome];
		if (outcome) view.outcome = outcome;
		return view;
	}
}
