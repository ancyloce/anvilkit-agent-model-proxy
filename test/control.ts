// A fake Control DispatchService (grpc-js, in process) with the single-use
// semantics the scenarios need: the first admission of a call is allowed,
// every later one answers the recorded dispatch without permission, a
// denial is recorded under the call, observations are deduplicated by
// source and sequence. Faults are scripted: a lost admission answer (the
// permission is consumed, the response never arrives) and unavailability.
// It is a scenario double of the real Control; the parent's integration
// scenarios use the real one.

import {
	type AdmitModelRequest,
	AdmitModelResponse,
	DispatchOutcome,
	DispatchServiceService,
	DispatchState,
	type GetDispatchRequest,
	GetDispatchResponse,
	type ObserveDispatchRequest,
	ObserveDispatchResponse,
} from "@anvilkit/generated-clients/proto/anvilkit/control/v1/dispatch";
import { Server, ServerCredentials, type ServerUnaryCall, type sendUnaryData, status } from "@grpc/grpc-js";

interface Recorded {
	dispatchId: string;
	callId: string;
	owner: string;
	tenantId: string;
	digest: string;
	state: DispatchState;
	outcome: DispatchOutcome;
	denialCode?: string;
	observations: {
		source: string;
		sequence: string;
		outcome: DispatchOutcome;
		usage?: Record<string, string>;
		nativeReference: string;
	}[];
}

export class FakeControl {
	readonly dispatches = new Map<string, Recorded>();
	readonly admits: AdmitModelRequest[] = [];
	readonly observes: ObserveDispatchRequest[] = [];
	/** Routes that are denied with the code (PROFILE_UNQUALIFIED, BUDGET_EXHAUSTED, ...). */
	readonly denials = new Map<string, string>();
	/** The next admissions that consume the permission but whose answer is lost (UNAVAILABLE after commit). */
	loseAdmitAnswers = 0;
	/** The next calls that fail before Control does anything. */
	unavailable = 0;
	/** The next observations that are recorded but whose answer is lost. */
	loseObserveAnswers = 0;
	/** Admission answers to hold until release (for duplicate races). */
	holdAdmits?: () => Promise<void>;
	private server!: Server;
	address = "";
	private seq = 0;

	async start(): Promise<this> {
		this.server = new Server();
		this.server.addService(DispatchServiceService, {
			admitModel: (
				call: ServerUnaryCall<AdmitModelRequest, AdmitModelResponse>,
				cb: sendUnaryData<AdmitModelResponse>,
			) => void this.admit(call.request, cb),
			observeDispatch: (
				call: ServerUnaryCall<ObserveDispatchRequest, ObserveDispatchResponse>,
				cb: sendUnaryData<ObserveDispatchResponse>,
			) => this.observe(call.request, cb),
			getDispatch: (
				call: ServerUnaryCall<GetDispatchRequest, GetDispatchResponse>,
				cb: sendUnaryData<GetDispatchResponse>,
			) => this.get(call.request, cb),
			admitTool: (_call: unknown, cb: sendUnaryData<never>) =>
				cb({ code: status.UNIMPLEMENTED, details: "not in the fake" } as never, null),
			confirmNotSent: (_call: unknown, cb: sendUnaryData<never>) =>
				cb({ code: status.UNIMPLEMENTED, details: "not in the fake" } as never, null),
		});
		const port = await new Promise<number>((resolve, reject) =>
			this.server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (err, p) =>
				err ? reject(err) : resolve(p),
			),
		);
		this.address = `127.0.0.1:${port}`;
		return this;
	}

	async stop(): Promise<void> {
		this.server.forceShutdown();
	}

	private view(r: Recorded) {
		return {
			dispatchId: r.dispatchId,
			callId: r.callId,
			owner: r.owner,
			state: r.state,
			outcome: r.outcome,
			reservedExposure: { currency: "USD", amount: "1000" },
			meterRevision: "fake",
			deadline: new Date(Date.now() + 60_000),
			admittedAt: new Date(),
		};
	}

	private async admit(req: AdmitModelRequest, cb: sendUnaryData<AdmitModelResponse>): Promise<void> {
		this.admits.push(req);
		if (this.unavailable > 0) {
			this.unavailable--;
			cb({ code: status.UNAVAILABLE, details: "fake control unavailable" } as never, null);
			return;
		}
		if (this.holdAdmits) await this.holdAdmits();
		const cmd = req.command;
		const key = `${cmd?.tenantId}/${req.owner}/${req.callId}`;
		const existing = this.dispatches.get(key);
		if (existing) {
			if (existing.digest !== cmd?.requestDigest) {
				cb(
					{
						code: status.ABORTED,
						details: `IDEMPOTENCY_CONFLICT: call ${req.callId} was admitted with another digest`,
					} as never,
					null,
				);
				return;
			}
			cb(
				null,
				AdmitModelResponse.fromPartial({
					admission: { dispatch: this.view(existing), dispatchAllowed: false, denialCode: existing.denialCode },
				}),
			);
			return;
		}
		this.seq++;
		const r: Recorded = {
			dispatchId: `dsp_fake_${this.seq}`,
			callId: req.callId,
			owner: req.owner,
			tenantId: cmd?.tenantId ?? "",
			digest: cmd?.requestDigest ?? "",
			state: DispatchState.DISPATCH_STATE_AUTHORIZED,
			outcome: DispatchOutcome.DISPATCH_OUTCOME_UNSPECIFIED,
			observations: [],
		};
		const denial = this.denials.get(req.routeId);
		if (denial) {
			r.state = DispatchState.DISPATCH_STATE_DENIED;
			r.denialCode = denial;
			this.dispatches.set(key, r);
			cb(
				null,
				AdmitModelResponse.fromPartial({
					admission: { dispatch: this.view(r), dispatchAllowed: false, denialCode: denial },
				}),
			);
			return;
		}
		this.dispatches.set(key, r);
		if (this.loseAdmitAnswers > 0) {
			this.loseAdmitAnswers--;
			cb(
				{
					code: status.UNAVAILABLE,
					details: "fake: the admission answer was lost after the permission was consumed",
				} as never,
				null,
			);
			return;
		}
		cb(null, AdmitModelResponse.fromPartial({ admission: { dispatch: this.view(r), dispatchAllowed: true } }));
	}

	private observe(req: ObserveDispatchRequest, cb: sendUnaryData<ObserveDispatchResponse>): void {
		this.observes.push(req);
		if (this.unavailable > 0) {
			this.unavailable--;
			cb({ code: status.UNAVAILABLE, details: "fake control unavailable" } as never, null);
			return;
		}
		const r = [...this.dispatches.values()].find((d) => d.dispatchId === req.dispatchId);
		if (!r) {
			cb({ code: status.NOT_FOUND, details: `NOT_FOUND: dispatch ${req.dispatchId}` } as never, null);
			return;
		}
		if (r.state === DispatchState.DISPATCH_STATE_DENIED) {
			cb(
				{
					code: status.FAILED_PRECONDITION,
					details: `STALE_EXECUTION: dispatch ${req.dispatchId} is denied; no permission was issued for a send`,
				} as never,
				null,
			);
			return;
		}
		const dup = r.observations.find((o) => o.source === req.source && o.sequence === req.sequence);
		if (dup) {
			cb(null, ObserveDispatchResponse.fromPartial({ dispatch: this.view(r), existing: true }));
			return;
		}
		if (req.outcome !== DispatchOutcome.DISPATCH_OUTCOME_UNKNOWN && !req.cumulativeUsage) {
			cb(
				{
					code: status.FAILED_PRECONDITION,
					details: `EFFECT_UNCERTAIN: dispatch ${req.dispatchId} reported a definite outcome without usage`,
				} as never,
				null,
			);
			return;
		}
		const usage = req.cumulativeUsage
			? {
					input: req.cumulativeUsage.inputUnits,
					output: req.cumulativeUsage.outputUnits,
					reasoning: req.cumulativeUsage.reasoningUnits,
					cached: req.cumulativeUsage.cachedInputUnits,
				}
			: undefined;
		r.observations.push({
			source: req.source,
			sequence: req.sequence,
			outcome: req.outcome,
			usage,
			nativeReference: req.nativeReference,
		});
		if (r.state !== DispatchState.DISPATCH_STATE_OBSERVED) {
			r.outcome = req.outcome;
			r.state =
				req.outcome === DispatchOutcome.DISPATCH_OUTCOME_UNKNOWN
					? DispatchState.DISPATCH_STATE_UNKNOWN
					: DispatchState.DISPATCH_STATE_OBSERVED;
		}
		if (this.loseObserveAnswers > 0) {
			this.loseObserveAnswers--;
			cb(
				{ code: status.UNAVAILABLE, details: "fake: the observation answer was lost after it was recorded" } as never,
				null,
			);
			return;
		}
		cb(null, ObserveDispatchResponse.fromPartial({ dispatch: this.view(r), existing: false }));
	}

	private get(req: GetDispatchRequest, cb: sendUnaryData<GetDispatchResponse>): void {
		const r = [...this.dispatches.values()].find(
			(d) => d.dispatchId === req.dispatchId || (req.callId && d.callId === req.callId && d.owner === req.owner),
		);
		if (!r) {
			cb({ code: status.NOT_FOUND, details: "NOT_FOUND: no such dispatch" } as never, null);
			return;
		}
		cb(null, GetDispatchResponse.fromPartial({ dispatch: this.view(r) }));
	}

	byCall(callId: string): Recorded | undefined {
		return [...this.dispatches.values()].find((d) => d.callId === callId);
	}
}
