// The Proxy's grpc-js client of anvilkit.control.v1.DispatchService (DD-02
// §4): the single-use model admission, the idempotent outcome/usage
// observation and the original-identity query. Every request passes the
// contract's explicit TypeScript validation (protovalidate over the
// generated descriptors) before it is sent; Control's public refusal code
// is carried in front of the status message, as the Go callers read it.
import { readFileSync } from "node:fs";
import {
	AdmitModelRequest,
	DispatchOutcome,
	DispatchServiceClient,
	DispatchState,
	GetDispatchRequest,
	ObserveDispatchRequest,
} from "@anvilkit/generated-clients/proto/anvilkit/control/v1/dispatch";
import { validateJson } from "@anvilkit/generated-clients/validation/rpc";
import { type ChannelCredentials, credentials, Metadata, type ServiceError, status } from "@grpc/grpc-js";
import type { Config } from "../config.js";
import type { ExecutionBinding, Outcome, Usage } from "../contracts.js";

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
	getDispatch(dispatchId: string): Promise<DispatchView>;
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

/** Maps a gRPC status to the Proxy's errors, as the Go callers' refusal() does. */
function refusal(err: ServiceError): Error {
	switch (err.code) {
		case status.FAILED_PRECONDITION:
		case status.ABORTED:
		case status.INVALID_ARGUMENT:
		case status.NOT_FOUND:
		case status.PERMISSION_DENIED:
		case status.RESOURCE_EXHAUSTED:
		case status.DATA_LOSS: {
			const code = (err.details || "").split(":")[0]?.trim() || status[err.code] || "REFUSED";
			return new ControlRefused(code, err.details || err.message);
		}
		default:
			return new ControlUnavailable(`control ${status[err.code] ?? err.code}: ${err.details || err.message}`);
	}
}

function channelCredentials(cfg: Config["control"]["identity"]): ChannelCredentials {
	if (cfg.mode === "mtls") {
		const m = cfg.mtls;
		return credentials.createSsl(readFileSync(m.caFile), readFileSync(m.keyFile), readFileSync(m.certFile), {
			checkServerIdentity: m.serverName
				? (_host, cert) =>
						cert.subject?.CN === m.serverName
							? undefined
							: new Error(`server name ${cert.subject?.CN} is not ${m.serverName}`)
				: undefined,
		});
	}
	return credentials.createInsecure();
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

	constructor(
		address: string,
		private readonly timeoutMs: number,
		identity: Config["control"]["identity"],
	) {
		this.client = new DispatchServiceClient(address, channelCredentials(identity), {
			// The Proxy never lets the channel retry a unary call by itself: an
			// admission asked twice is the same durable command, and a lost
			// answer is reentered by the caller under the same identity.
			"grpc.enable_retries": 0,
		});
	}

	close(): void {
		this.client.close();
	}

	private call<Req, Res>(
		fn: (req: Req, md: Metadata, opts: { deadline: Date }, cb: (err: ServiceError | null, res: Res) => void) => unknown,
		req: Req,
	): Promise<Res> {
		return new Promise<Res>((resolve, reject) => {
			fn.call(this.client, req, new Metadata(), { deadline: new Date(Date.now() + this.timeoutMs) }, (err, res) => {
				if (err) reject(refusal(err));
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
		const res = await this.call(this.client.admitModel, req);
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
		const res = await this.call(this.client.observeDispatch, req);
		if (!res.dispatch) throw new ControlUnavailable("Control answered no dispatch");
		return { existing: res.existing, state: stateOf(res.dispatch.state) };
	}

	async getDispatch(dispatchId: string): Promise<DispatchView> {
		const req = GetDispatchRequest.fromPartial({ dispatchId });
		validated("anvilkit.control.v1.GetDispatchRequest", GetDispatchRequest.toJSON(req));
		const res = await this.call(this.client.getDispatch, req);
		const d = res.dispatch;
		if (!d) throw new ControlUnavailable("Control answered no dispatch");
		const view: DispatchView = { dispatchId: d.dispatchId, callId: d.callId, owner: d.owner, state: stateOf(d.state) };
		const outcome = outcomeFromProto[d.outcome];
		if (outcome) view.outcome = outcome;
		return view;
	}
}
