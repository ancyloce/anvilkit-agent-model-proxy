// The countable controlled upstream of the scenarios: an OpenAI-compatible
// chat-completions double that records every physical receive (headers and
// body) and answers as scripted. It stands in for a provider; nothing here
// is a paid-provider claim.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { encode } from "eventsource-encoder";

export interface Receive {
	url: string;
	headers: Record<string, string>;
	body: string;
}

export type Behavior =
	| {
			kind: "stream";
			text?: string[];
			toolCall?: { id: string; name: string; arguments: string };
			/** The usage member of the final chunk as the double reports it (any value); null: no usage chunk. */
			usage?: unknown;
			finish?: string;
			id?: string;
			/** Writes the whole answer in fragments of this many bytes (cutting events, lines and multibyte sequences). */
			fragmentBytes?: number;
	  }
	| { kind: "status"; status: number; body?: string }
	| { kind: "redirect"; location: string }
	| { kind: "close" }
	| { kind: "hang" }
	| { kind: "stream-then-hang"; text: string[] };

export class Upstream {
	readonly receives: Receive[] = [];
	readonly redirectTargetReceives: Receive[] = [];
	private readonly script: Behavior[] = [];
	private server!: Server;
	private target!: Server;
	url = "";
	targetUrl = "";
	/** Open responses of hanging behaviors, ended by release(). */
	private hanging: ServerResponse[] = [];

	next(...b: Behavior[]): this {
		this.script.push(...b);
		return this;
	}

	async start(): Promise<this> {
		this.target = createServer((req, res) => {
			this.collect(req, (r) => {
				this.redirectTargetReceives.push(r);
				res.writeHead(200, { "content-type": "application/json" }).end("{}");
			});
		});
		await new Promise<void>((r) => this.target.listen(0, "127.0.0.1", r));
		this.targetUrl = `http://127.0.0.1:${(this.target.address() as AddressInfo).port}`;
		this.server = createServer((req, res) => this.handle(req, res));
		await new Promise<void>((r) => this.server.listen(0, "127.0.0.1", r));
		this.url = `http://127.0.0.1:${(this.server.address() as AddressInfo).port}`;
		return this;
	}

	async stop(): Promise<void> {
		this.release();
		await new Promise<void>((r) => this.server.close(() => r()));
		await new Promise<void>((r) => this.target.close(() => r()));
	}

	/** Drops the listener so a connection attempt fails. */
	async closeListener(): Promise<void> {
		await new Promise<void>((r) => this.server.close(() => r()));
	}

	release(): void {
		for (const r of this.hanging) r.end();
		this.hanging = [];
	}

	/** Resolves once the double holds an answer open: the observable fact that the Proxy's send is in flight. */
	async awaitHeld(): Promise<void> {
		const until = Date.now() + 20_000;
		while (this.hanging.length === 0) {
			if (Date.now() > until) throw new Error("the upstream never held a send");
			await new Promise((r) => setTimeout(r, 10));
		}
	}

	private collect(req: IncomingMessage, cb: (r: Receive) => void): void {
		const chunks: Buffer[] = [];
		req.on("data", (c: Buffer) => chunks.push(c));
		req.on("end", () => {
			const headers: Record<string, string> = {};
			for (const [k, v] of Object.entries(req.headers)) headers[k] = Array.isArray(v) ? v.join(",") : (v ?? "");
			cb({ url: req.url ?? "", headers, body: Buffer.concat(chunks).toString("utf8") });
		});
	}

	private handle(req: IncomingMessage, res: ServerResponse): void {
		this.collect(req, (r) => {
			this.receives.push(r);
			const b = this.script.shift() ?? { kind: "stream" as const };
			switch (b.kind) {
				case "status":
					res
						.writeHead(b.status, { "content-type": "application/json" })
						.end(b.body ?? JSON.stringify({ error: { message: "scripted failure" } }));
					return;
				case "redirect":
					res.writeHead(307, { location: b.location }).end();
					return;
				case "close":
					res.destroy();
					return;
				case "hang":
					this.hanging.push(res);
					return;
				case "stream-then-hang":
					res.writeHead(200, { "content-type": "text/event-stream" });
					for (const t of b.text) res.write(this.chunk({ delta: { content: t }, finish_reason: null }));
					this.hanging.push(res);
					return;
				case "stream":
					this.stream(res, b);
					return;
			}
		});
	}

	/** One chat.completion.chunk as an SSE event (eventsource-encoder frames it). */
	private chunk(choice: Record<string, unknown>, extra: Record<string, unknown> = {}, id = "chatcmpl-fixture"): string {
		return encode({
			data: JSON.stringify({
				id,
				object: "chat.completion.chunk",
				created: 1,
				model: "fixture-model",
				choices: [{ index: 0, ...choice }],
				...extra,
			}),
		});
	}

	private stream(res: ServerResponse, b: Extract<Behavior, { kind: "stream" }>): void {
		const id = b.id ?? "chatcmpl-fixture";
		res.writeHead(200, { "content-type": "text/event-stream", "x-request-id": "req-fixture" });
		let out = this.chunk({ delta: { role: "assistant", content: "" }, finish_reason: null }, {}, id);
		for (const t of b.text ?? ["Hello", " from", " the fixture"]) {
			out += this.chunk({ delta: { content: t }, finish_reason: null }, {}, id);
		}
		if (b.toolCall) {
			out += this.chunk(
				{
					delta: {
						tool_calls: [
							{ index: 0, id: b.toolCall.id, type: "function", function: { name: b.toolCall.name, arguments: "" } },
						],
					},
					finish_reason: null,
				},
				{},
				id,
			);
			out += this.chunk(
				{ delta: { tool_calls: [{ index: 0, function: { arguments: b.toolCall.arguments } }] }, finish_reason: null },
				{},
				id,
			);
		}
		out += this.chunk({ delta: {}, finish_reason: b.finish ?? (b.toolCall ? "tool_calls" : "stop") }, {}, id);
		if (b.usage !== null) {
			const usage = b.usage ?? {
				prompt_tokens: 12,
				completion_tokens: 5,
				total_tokens: 17,
				prompt_tokens_details: { cached_tokens: 0 },
				completion_tokens_details: { reasoning_tokens: 0 },
			};
			out += encode({
				data: JSON.stringify({
					id,
					object: "chat.completion.chunk",
					created: 1,
					model: "fixture-model",
					choices: [],
					usage,
				}),
			});
		}
		out += encode({ data: "[DONE]" });
		if (b.fragmentBytes) {
			// The bytes leave in pieces that cut lines and multibyte sequences:
			// what a network delivers, and what the parser must reassemble.
			const bytes = Buffer.from(out, "utf8");
			let i = 0;
			const next = () => {
				if (i >= bytes.length) {
					res.end();
					return;
				}
				res.write(bytes.subarray(i, i + (b.fragmentBytes as number)));
				i += b.fragmentBytes as number;
				setImmediate(next);
			};
			next();
			return;
		}
		res.write(out);
		res.end();
	}
}
