// The pi-ai adapter (B05, DD-03 §2 ControlledModelPort): protocol
// adaptation of one call on one route through the pinned SDK's
// openai-completions path — the only API path this build implements and
// inspected (pi-ai 0.85.1 api/openai-completions.js: one OpenAI client per
// call built on the fetch the request option names; the SDK is called with
// maxRetries 0 and pi's own retry wrapper retries nothing at maxRetries 0;
// no fallback of its own). The model definition is built here from the
// reviewed route, never from pi-ai's model registry or a caller's word; the
// API key is the route credential; the transport is the ticket's fetch;
// every request option that could add headers, sessions, caching or
// retries is fixed. Thinking content is not part of the frozen frame
// contract and is left to the native evidence.
import type { AssistantMessage, Context, Model, Message as PiMessage, Tool, ToolCall } from "@earendil-works/pi-ai";
import { stream as streamOpenAICompletions } from "@earendil-works/pi-ai/api/openai-completions";
import type { Route, ToolSchema } from "../config.js";
import { type Message, parseStrictJson, type ToolDefinition } from "../contracts.js";
import type { SendTicket } from "./transport.js";

export type UpstreamEvent =
	| { type: "text"; delta: string }
	| { type: "tool_call"; id: string; name: string; arguments: string }
	| { type: "done"; reason: "stop" | "length" | "toolUse" | "deferred"; message: AssistantMessage }
	| { type: "error"; reason: "aborted" | "error"; message: string; final: AssistantMessage };

export interface UpstreamInput {
	route: Route;
	credential: string;
	messages: Message[];
	tools: ToolDefinition[];
	maxOutputTokens: number;
	timeoutMs: number;
	signal: AbortSignal;
	ticket: SendTicket;
}

/** The pi-ai model of a route: explicit compatibility, no URL-based detection, no cost (Control meters). */
export function modelOf(route: Route): Model<"openai-completions"> {
	return {
		id: route.model,
		name: route.model,
		api: "openai-completions",
		provider: route.provider,
		baseUrl: route.baseUrl,
		reasoning: false,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 1_000_000,
		maxTokens: route.limits.maxOutputTokens,
		compat: {
			supportsStore: false,
			supportsDeveloperRole: false,
			supportsReasoningEffort: false,
			supportsUsageInStreaming: true,
			supportsFinishReason: true,
			maxTokensField: "max_tokens",
			requiresToolResultName: false,
			requiresAssistantAfterToolResult: false,
			requiresThinkingAsText: false,
			requiresReasoningContentOnAssistantMessages: false,
			thinkingFormat: "openai",
			zaiToolStream: false,
			supportsThinkingTokenBudget: false,
			supportsStrictMode: false,
			supportsOpenAIGrammarTools: false,
			sendSessionAffinityHeaders: false,
			supportsLongCacheRetention: false,
		},
	};
}

function toolOf(schema: ToolSchema): Tool {
	return {
		name: schema.name,
		description: schema.description,
		parameters: schema.inputSchema as unknown as Tool["parameters"],
	};
}

const zeroUsage = {
	input: 0,
	output: 0,
	cacheRead: 0,
	cacheWrite: 0,
	totalTokens: 0,
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

/**
 * The contract messages as a pi-ai context: the system role becomes the
 * system prompt; an assistant message keeps its tool calls (ids, names and
 * arguments) as tool-call blocks and a tool message is the result of the
 * assistant call it names, so the next request replays the round trip the
 * model produced. Assistant messages are marked as this route's own
 * (provider, api, model): pi-ai then replays their tool-call ids untouched.
 * The application validated the messages before (a tool result names an
 * earlier assistant tool call, arguments are JSON objects).
 */
export function contextOf(route: Route, messages: Message[], tools: ToolSchema[]): Context {
	const system: string[] = [];
	const out: PiMessage[] = [];
	const toolNames = new Map<string, string>();
	const now = Date.now();
	for (const m of messages) {
		switch (m.role) {
			case "system":
				system.push(m.content);
				break;
			case "user":
				out.push({ role: "user", content: m.content, timestamp: now });
				break;
			case "assistant": {
				const content: AssistantMessage["content"] = [];
				if (m.content.length > 0) content.push({ type: "text", text: m.content });
				for (const tc of m.toolCalls ?? []) {
					toolNames.set(tc.toolCallId, tc.name);
					const call: ToolCall = {
						type: "toolCall",
						id: tc.toolCallId,
						name: tc.name,
						arguments: parseStrictJson(tc.arguments) as Record<string, unknown>,
					};
					content.push(call);
				}
				out.push({
					role: "assistant",
					content,
					api: "openai-completions",
					provider: route.provider,
					model: route.model,
					usage: { ...zeroUsage, cost: { ...zeroUsage.cost } },
					stopReason: (m.toolCalls?.length ?? 0) > 0 ? "toolUse" : "stop",
					timestamp: now,
				});
				break;
			}
			case "tool":
				out.push({
					role: "toolResult",
					toolCallId: m.toolCallId ?? "",
					toolName: toolNames.get(m.toolCallId ?? "") ?? "",
					content: [{ type: "text", text: m.content }],
					isError: false,
					timestamp: now,
				});
				break;
		}
	}
	const ctx: Context = { messages: out };
	if (system.length > 0) ctx.systemPrompt = system.join("\n\n");
	if (tools.length > 0) ctx.tools = tools.map(toolOf);
	return ctx;
}

/**
 * One physical send through pi-ai's openai-completions path on the ticket's
 * fetch: the events are the SDK's, re-expressed for normalization.
 */
export async function* streamUpstream(input: UpstreamInput): AsyncGenerator<UpstreamEvent> {
	const byName = new Map(input.route.tools.map((t) => [t.name, t]));
	const tools = input.tools.map((t) => {
		const schema = byName.get(t.name);
		if (!schema) throw new Error(`tool ${t.name} is not a reviewed tool of route ${input.route.id}`);
		return schema;
	});
	const model = modelOf(input.route);
	const context = contextOf(input.route, input.messages, tools);
	const stream = streamOpenAICompletions(model, context, {
		apiKey: input.credential,
		fetch: input.ticket.fetch,
		// The wire carries the service's name, not the host's platform details.
		headers: { "User-Agent": "anvilkit-agent-model-proxy" },
		maxTokens: input.maxOutputTokens,
		maxRetries: 0,
		timeoutMs: input.timeoutMs,
		signal: input.signal,
		cacheRetention: "none",
	});
	for await (const ev of stream) {
		switch (ev.type) {
			case "text_delta":
				yield { type: "text", delta: ev.delta };
				break;
			case "toolcall_end":
				yield {
					type: "tool_call",
					id: ev.toolCall.id,
					name: ev.toolCall.name,
					arguments: JSON.stringify(ev.toolCall.arguments ?? {}),
				};
				break;
			case "done":
				yield { type: "done", reason: ev.reason, message: ev.message };
				return;
			case "error":
				yield { type: "error", reason: ev.reason, message: ev.error.errorMessage ?? "upstream error", final: ev.error };
				return;
			default:
				break;
		}
	}
}
