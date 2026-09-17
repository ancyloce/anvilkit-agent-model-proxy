// The contract boundary of the Model Proxy transport (contracts.md §1, §4):
// request bodies are parsed strictly (duplicate members, non-finite and
// lossy numbers rejected before any schema runs) and validated against the
// component schemas of openapi/model-proxy.yaml, the frozen source of this
// surface, read from the contracts directory named by the environment or
// found beside this package in the parent checkout — the same file the Go
// consumers embed. The TypeScript types are the generated consumers of that
// file (@anvilkit/generated-clients); nothing here restates the contract.
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { components } from "@anvilkit/generated-clients/openapi/model-proxy";
import { parseStrictJson } from "@anvilkit/generated-clients/validation/json";
import { Ajv, type ValidateFunction } from "ajv";
import { parse as parseYaml } from "yaml";

export type Schemas = components["schemas"];
export type ModelCallRequest = Schemas["ModelCallRequest"];
export type ModelCall = Schemas["ModelCall"];
export type StreamFrame = Schemas["StreamFrame"];
export type Usage = Schemas["Usage"];
export type ErrorEnvelope = Schemas["ErrorEnvelope"];
export type ErrorCode = ErrorEnvelope["error"]["code"];
export type ExecutionBinding = Schemas["ExecutionBinding"];
export type Message = Schemas["Message"];
export type MessageToolCall = Schemas["MessageToolCall"];
export type ToolDefinition = Schemas["ToolDefinition"];
export type ModelCallState = ModelCall["state"];
export type Outcome = NonNullable<StreamFrame["outcome"]>;

export { parseStrictJson };

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

/** The directory holding openapi/model-proxy.yaml (and its fixtures). */
export function contractsDir(configured?: string): string {
	const env = configured || process.env.ANVILKIT_MODEL_PROXY_CONTRACTS_DIR;
	if (env) return env;
	for (const candidate of [
		path.join(packageRoot, "contracts"),
		path.resolve(packageRoot, "..", "..", "..", "contracts"),
	]) {
		if (existsSync(path.join(candidate, "openapi", "model-proxy.yaml"))) return candidate;
	}
	throw new Error(
		"contracts not found: set ANVILKIT_MODEL_PROXY_CONTRACTS_DIR to a directory holding openapi/model-proxy.yaml",
	);
}

export class ContractViolation extends Error {
	constructor(
		readonly schema: string,
		readonly details: string[],
	) {
		super(`${schema}: ${details.join("; ")}`);
	}
}

/** The compiled validators of one contract document. */
export class Contract {
	private readonly validators = new Map<string, ValidateFunction>();
	private readonly ajv: Ajv;
	readonly schemas: Record<string, unknown>;

	constructor(dir?: string) {
		const file = path.join(contractsDir(dir), "openapi", "model-proxy.yaml");
		const doc = parseYaml(readFileSync(file, "utf8"), { uniqueKeys: true }) as {
			components?: { schemas?: Record<string, unknown> };
		};
		const schemas = doc.components?.schemas;
		if (!schemas || typeof schemas !== "object") {
			throw new Error(`${file}: no components.schemas`);
		}
		this.schemas = schemas;
		// OpenAPI 3.0 schema objects are the draft-07 subset the contract
		// restricts itself to (type, pattern, bounds, enum, required,
		// additionalProperties, items, $ref). The intra-document references
		// are rebound to one Ajv document so they resolve as written.
		const definitions = JSON.parse(JSON.stringify(schemas).replaceAll("#/components/schemas/", "#/definitions/"));
		this.ajv = new Ajv({ strict: true, allErrors: false, allowUnionTypes: false });
		this.ajv.addSchema({ $id: "urn:anvilkit:model-proxy:v1", definitions });
		for (const name of Object.keys(schemas)) {
			this.validators.set(name, this.ajv.compile({ $ref: `urn:anvilkit:model-proxy:v1#/definitions/${name}` }));
		}
	}

	/** Validates an already parsed document against the named component schema. */
	check(schema: string, value: unknown): string[] {
		const v = this.validators.get(schema);
		if (!v) throw new Error(`${schema} is not a component schema of the contract`);
		if (v(value)) return [];
		// Allowlisted detail: the instance path and Ajv's message; never the
		// offending value (it may be prompt text).
		return (v.errors ?? []).map((e) => `${e.instancePath || "/"} ${e.message ?? "invalid"}`);
	}

	/** Strict parse of `text` as the named schema; throws ContractViolation. */
	parse<T>(schema: string, text: string): T {
		let value: unknown;
		try {
			value = parseStrictJson(text);
		} catch (err) {
			throw new ContractViolation(schema, [`malformed JSON: ${(err as Error).message}`]);
		}
		const details = this.check(schema, value);
		if (details.length > 0) throw new ContractViolation(schema, details);
		return value as T;
	}
}
