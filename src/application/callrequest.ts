// The admission-free half of a call's validation (DD-03 §6): the reviewed
// route and its bounds, the tool round trip of the history and the digest of
// the immutable content a reentry must repeat. Nothing here reads a record,
// asks Control or sends.

import type { Config, Route } from "../config.js";
import { routeDisabledReasons } from "../config.js";
import { type ModelCallRequest, parseStrictJson } from "../contracts.js";
import { canonicalJson, digestOf, toolSchemaDigest } from "../domain/call.js";
import { CallError } from "./callerror.js";

export function routeFor(cfg: Config, id: string): Route {
	const route = cfg.routes.find((r) => r.id === id);
	if (!route) throw new CallError("PROFILE_UNQUALIFIED", `route ${id} is not a reviewed route of this Proxy`);
	const reasons = routeDisabledReasons(cfg, route);
	if (reasons.length > 0)
		throw new CallError("PROFILE_UNQUALIFIED", `route ${id} is not enabled (${reasons.join(", ")})`);
	return route;
}

/** The digest of the immutable content of a request: what a reentry must repeat exactly. */
export function contentDigestOf(req: ModelCallRequest): string {
	return digestOf(
		canonicalJson({
			binding: req.binding,
			routeId: req.routeId,
			messages: req.messages,
			tools: req.tools ?? [],
			maxOutputTokens: req.maxOutputTokens,
			maxExposure: req.maxExposure,
			deadline: req.deadline,
			supersedesCallId: req.supersedesCallId ?? "",
			evidenceRef: req.evidenceRef ?? "",
		}),
	);
}

/** Validates a new call against the route profile; returns the route and the deadline. */
export function validateCall(
	req: ModelCallRequest,
	cfg: Config,
	health: { healthy(routeId: string): boolean } | undefined,
	now: () => number,
): { route: Route; deadlineMs: number } {
	const route = routeFor(cfg, req.routeId);
	if (health && !health.healthy(route.id))
		throw new CallError(
			"DEPENDENCY_UNAVAILABLE",
			`route ${route.id} failed its latest health probe; no send was attempted`,
			true,
		);
	const nowMs = now();
	const deadlineMs = Date.parse(req.deadline);
	if (Number.isNaN(deadlineMs)) throw new CallError("INVALID_ARGUMENT", "deadline is not a timestamp");
	if (deadlineMs <= nowMs) throw new CallError("STALE_EXECUTION", "the call deadline has passed");
	if (deadlineMs > nowMs + route.limits.maxDeadlineMs) {
		throw new CallError(
			"INVALID_ARGUMENT",
			`the call deadline extends the route bound of ${route.limits.maxDeadlineMs}ms`,
		);
	}
	if (req.maxOutputTokens > route.limits.maxOutputTokens) {
		throw new CallError(
			"INVALID_ARGUMENT",
			`maxOutputTokens ${req.maxOutputTokens} exceeds the route bound ${route.limits.maxOutputTokens}`,
		);
	}
	if (req.maxExposure.currency !== route.limits.maxExposure.currency) {
		throw new CallError(
			"INVALID_ARGUMENT",
			`the route is bounded in ${route.limits.maxExposure.currency}, the exposure is declared in ${req.maxExposure.currency}`,
		);
	}
	const amount = BigInt(req.maxExposure.amount);
	if (amount <= 0n) throw new CallError("INVALID_ARGUMENT", "maxExposure must be positive");
	if (amount > BigInt(route.limits.maxExposure.amount)) {
		throw new CallError(
			"INVALID_ARGUMENT",
			`maxExposure ${req.maxExposure.amount} exceeds the route bound ${route.limits.maxExposure.amount}`,
		);
	}
	const names = new Set<string>();
	for (const t of req.tools ?? []) {
		if (names.has(t.name)) throw new CallError("INVALID_ARGUMENT", `tool ${t.name} is named twice`);
		names.add(t.name);
		const schema = route.tools.find((s) => s.name === t.name);
		if (!schema)
			throw new CallError("PROFILE_UNQUALIFIED", `tool ${t.name} is not a reviewed tool of route ${route.id}`);
		if (toolSchemaDigest(schema.inputSchema) !== t.inputSchemaDigest) {
			throw new CallError(
				"PROFILE_UNQUALIFIED",
				`tool ${t.name}: the schema digest does not match the reviewed schema of route ${route.id}`,
			);
		}
	}
	validateToolHistory(req, route);
	return { route, deadlineMs };
}

/**
 * The tool round trip of the message history: tool calls belong to
 * assistant messages, name a reviewed tool of the route with JSON-object
 * arguments and have distinct ids; a tool message answers exactly one
 * earlier assistant tool call; no other role carries either.
 */
export function validateToolHistory(req: ModelCallRequest, route: Route): void {
	const calls = new Map<string, string>();
	const answered = new Set<string>();
	req.messages.forEach((m, i) => {
		const at = `messages[${i}]`;
		if (m.role !== "assistant" && m.toolCalls !== undefined) {
			throw new CallError("INVALID_ARGUMENT", `${at}: toolCalls belong to assistant messages`);
		}
		if (m.role !== "tool" && m.toolCallId !== undefined) {
			throw new CallError("INVALID_ARGUMENT", `${at}: toolCallId belongs to tool messages`);
		}
		if (m.role === "assistant") {
			for (const tc of m.toolCalls ?? []) {
				if (calls.has(tc.toolCallId))
					throw new CallError("INVALID_ARGUMENT", `${at}: tool call ${tc.toolCallId} repeats`);
				if (!route.tools.some((s) => s.name === tc.name)) {
					throw new CallError(
						"PROFILE_UNQUALIFIED",
						`${at}: tool ${tc.name} is not a reviewed tool of route ${route.id}`,
					);
				}
				let args: unknown;
				try {
					args = parseStrictJson(tc.arguments);
				} catch (err) {
					throw new CallError("INVALID_ARGUMENT", `${at}: tool call ${tc.toolCallId}: ${(err as Error).message}`);
				}
				if (typeof args !== "object" || args === null || Array.isArray(args)) {
					throw new CallError("INVALID_ARGUMENT", `${at}: tool call ${tc.toolCallId}: arguments are not a JSON object`);
				}
				calls.set(tc.toolCallId, tc.name);
			}
		}
		if (m.role === "tool") {
			if (!m.toolCallId) throw new CallError("INVALID_ARGUMENT", `${at}: a tool message needs toolCallId`);
			if (!calls.has(m.toolCallId)) {
				throw new CallError(
					"INVALID_ARGUMENT",
					`${at}: tool call ${m.toolCallId} was not made by an earlier assistant message`,
				);
			}
			if (answered.has(m.toolCallId)) {
				throw new CallError("INVALID_ARGUMENT", `${at}: tool call ${m.toolCallId} is answered twice`);
			}
			answered.add(m.toolCallId);
		}
	});
}
