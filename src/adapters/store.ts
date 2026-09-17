// The Proxy's durable state: call records, private native evidence, the
// pending-observation markers and cancellation intents, as objects in an
// object store (DD-03 §6 "persist private native evidence and usage before
// idempotent observation submission"). Two backends, as Control's inventory
// has: the filesystem (DEVELOPMENT_ONLY, one host) and an S3-compatible
// bucket with its own credentials (the chart renders only this one). The
// store is transport state of the sender; Control's dispatch record is the
// ledger and never derives from it. Every object of a call is addressed by
// the call's scope (<kind>/<callId>/<tenantId>): a call id is unique per
// tenant, never globally, so two tenants' calls never share an object.
import { createHash } from "node:crypto";
import { mkdir, open, readdir, readFile, rename, rm, stat } from "node:fs/promises";
import path from "node:path";
import {
	DeleteObjectCommand,
	GetObjectCommand,
	ListObjectsV2Command,
	PutObjectCommand,
	S3Client,
	S3ServiceException,
} from "@aws-sdk/client-s3";
import type { Config } from "../config.js";
import type { CallRecord, CallScope, Settlement } from "../domain/call.js";

export class PreconditionFailed extends Error {}

export interface StoredObject {
	body: Uint8Array;
	version: string;
}

export interface PutOptions {
	/** Create only: fail with PreconditionFailed when the key exists. */
	ifNoneMatch?: boolean;
	/** Replace only the version last read: fail with PreconditionFailed otherwise. */
	ifMatch?: string;
}

export interface ObjectStore {
	get(key: string): Promise<StoredObject | undefined>;
	put(key: string, body: Uint8Array, opts?: PutOptions): Promise<string>;
	delete(key: string): Promise<void>;
	/** Every key under the prefix (complete enumeration). */
	list(prefix: string): Promise<string[]>;
	/** Proves the backend's semantics before the Proxy serves; throws otherwise. */
	qualify(): Promise<void>;
	describe(): string;
}

// A key is a kind and one or two contract ids (the call's scope is two: the
// call id, then the tenant); a list prefix is a kind alone or a kind and the
// first id. Ids never contain "/", so the segments are unambiguous.
const idSegment = "[A-Za-z0-9][A-Za-z0-9._:-]{0,127}";
const keyPattern = new RegExp(`^[a-z]+/${idSegment}(/${idSegment})?$`);
const prefixPattern = new RegExp(`^[a-z]+/(${idSegment}/)?$`);

function checkKey(key: string): void {
	if (!keyPattern.test(key) || key.split("/").some((s) => s === "." || s === "..")) {
		throw new Error(`invalid store key ${JSON.stringify(key)}`);
	}
}

function checkPrefix(prefix: string): void {
	if (!prefixPattern.test(prefix) || prefix.split("/").some((s) => s === "." || s === "..")) {
		throw new Error(`invalid list prefix ${JSON.stringify(prefix)}`);
	}
}

function versionOf(body: Uint8Array): string {
	return createHash("sha256").update(body).digest("hex");
}

/** DEVELOPMENT_ONLY: one directory on one host; versions are content hashes. */
export class FilesystemStore implements ObjectStore {
	constructor(readonly dir: string) {}

	describe(): string {
		return `filesystem ${this.dir} (DEVELOPMENT_ONLY)`;
	}

	private pathOf(key: string): string {
		checkKey(key);
		return path.join(this.dir, key);
	}

	async get(key: string): Promise<StoredObject | undefined> {
		try {
			const body = await readFile(this.pathOf(key));
			return { body, version: versionOf(body) };
		} catch (err) {
			if ((err as NodeJS.ErrnoException).code === "ENOENT") return undefined;
			throw err;
		}
	}

	async put(key: string, body: Uint8Array, opts: PutOptions = {}): Promise<string> {
		const target = this.pathOf(key);
		await mkdir(path.dirname(target), { recursive: true, mode: 0o750 });
		if (opts.ifNoneMatch) {
			let fh: Awaited<ReturnType<typeof open>>;
			try {
				fh = await open(target, "wx", 0o640);
			} catch (err) {
				if ((err as NodeJS.ErrnoException).code === "EEXIST") throw new PreconditionFailed(`${key} exists`);
				throw err;
			}
			try {
				await fh.writeFile(body);
				await fh.sync();
			} finally {
				await fh.close();
			}
			return versionOf(body);
		}
		if (opts.ifMatch !== undefined) {
			const current = await this.get(key);
			if (!current || current.version !== opts.ifMatch) throw new PreconditionFailed(`${key} changed`);
		}
		const tmp = `${target}.${process.pid}.${Date.now()}.tmp`;
		const fh = await open(tmp, "wx", 0o640);
		try {
			await fh.writeFile(body);
			await fh.sync();
		} finally {
			await fh.close();
		}
		await rename(tmp, target);
		return versionOf(body);
	}

	async delete(key: string): Promise<void> {
		await rm(this.pathOf(key), { force: true });
	}

	async list(prefix: string): Promise<string[]> {
		checkPrefix(prefix);
		const dir = path.join(this.dir, prefix);
		try {
			const entries = await readdir(dir, { recursive: true, withFileTypes: true });
			const out: string[] = [];
			for (const e of entries) {
				if (!e.isFile() || e.name.endsWith(".tmp")) continue;
				const rel = path.relative(dir, path.join(e.parentPath, e.name));
				out.push(prefix + rel.split(path.sep).join("/"));
			}
			return out.sort();
		} catch (err) {
			if ((err as NodeJS.ErrnoException).code === "ENOENT") return [];
			throw err;
		}
	}

	async qualify(): Promise<void> {
		await mkdir(this.dir, { recursive: true, mode: 0o750 });
		const st = await stat(this.dir);
		if (!st.isDirectory()) throw new Error(`${this.dir} is not a directory`);
	}
}

/** An S3-compatible bucket through the AWS SDK: conditional create and replace by ETag. */
export class S3Store implements ObjectStore {
	private readonly client: S3Client;
	private readonly bucket: string;
	private readonly prefix: string;
	private readonly endpoint: string;

	constructor(s3: Config["store"]["s3"]) {
		this.bucket = s3.bucket;
		this.prefix = s3.prefix ? `${s3.prefix.replace(/\/+$/, "")}/` : "";
		this.endpoint = s3.endpoint;
		this.client = new S3Client({
			endpoint: s3.endpoint,
			region: s3.region,
			forcePathStyle: s3.pathStyle,
			credentials: { accessKeyId: s3.accessKeyId, secretAccessKey: s3.secretAccessKey },
			maxAttempts: 3,
		});
	}

	describe(): string {
		return `s3 ${this.endpoint} bucket ${this.bucket} prefix ${this.prefix || "(none)"}`;
	}

	private keyOf(key: string): string {
		checkKey(key);
		return this.prefix + key;
	}

	async get(key: string): Promise<StoredObject | undefined> {
		try {
			const out = await this.client.send(new GetObjectCommand({ Bucket: this.bucket, Key: this.keyOf(key) }));
			const body = await out.Body?.transformToByteArray();
			if (!body) return undefined;
			return { body, version: out.ETag ?? versionOf(body) };
		} catch (err) {
			if (err instanceof S3ServiceException && (err.name === "NoSuchKey" || err.$metadata.httpStatusCode === 404))
				return undefined;
			throw err;
		}
	}

	async put(key: string, body: Uint8Array, opts: PutOptions = {}): Promise<string> {
		try {
			const out = await this.client.send(
				new PutObjectCommand({
					Bucket: this.bucket,
					Key: this.keyOf(key),
					Body: body,
					ContentType: "application/json",
					ContentMD5: createHash("md5").update(body).digest("base64"),
					...(opts.ifNoneMatch ? { IfNoneMatch: "*" } : {}),
					...(opts.ifMatch !== undefined ? { IfMatch: opts.ifMatch } : {}),
				}),
			);
			return out.ETag ?? versionOf(body);
		} catch (err) {
			if (
				err instanceof S3ServiceException &&
				(err.name === "PreconditionFailed" || err.$metadata.httpStatusCode === 412)
			) {
				throw new PreconditionFailed(`${key}: ${err.name}`);
			}
			throw err;
		}
	}

	async delete(key: string): Promise<void> {
		await this.client.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: this.keyOf(key) }));
	}

	async list(prefix: string): Promise<string[]> {
		checkPrefix(prefix);
		const out: string[] = [];
		let token: string | undefined;
		do {
			const page = await this.client.send(
				new ListObjectsV2Command({ Bucket: this.bucket, Prefix: this.prefix + prefix, ContinuationToken: token }),
			);
			for (const o of page.Contents ?? []) {
				if (o.Key) out.push(o.Key.slice(this.prefix.length));
			}
			token = page.IsTruncated ? page.NextContinuationToken : undefined;
		} while (token);
		return out.sort();
	}

	/** Conditional create, same-key conflict, replace by version and read-after-write against the real backend. */
	async qualify(): Promise<void> {
		const key = `probe/qualify-${process.pid}-${Date.now()}`;
		const body = new TextEncoder().encode(JSON.stringify({ probe: key }));
		const v1 = await this.put(key, body, { ifNoneMatch: true });
		let conflicted = false;
		try {
			await this.put(key, body, { ifNoneMatch: true });
		} catch (err) {
			conflicted = err instanceof PreconditionFailed;
		}
		if (!conflicted)
			throw new Error("the S3 backend did not refuse a conditional create of an existing key (If-None-Match)");
		const read = await this.get(key);
		if (!read || read.version !== v1)
			throw new Error("the S3 backend did not return the written object with its version");
		let stale = false;
		try {
			await this.put(key, body, { ifMatch: '"not-the-version"' });
		} catch (err) {
			stale = err instanceof PreconditionFailed;
		}
		if (!stale) throw new Error("the S3 backend did not refuse a replace with a stale version (If-Match)");
		const listed = await this.list("probe/");
		if (!listed.includes(key)) throw new Error("the S3 backend did not list the written key");
		await this.delete(key);
	}
}

export function newObjectStore(cfg: Config): ObjectStore {
	return cfg.store.backend === "s3" ? new S3Store(cfg.store.s3) : new FilesystemStore(cfg.store.dir);
}

/**
 * Native evidence of one send: the request as sent (without credentials),
 * the raw response, and the settlement the sender derived from them — the
 * outcome, usage, native reference and normalized frames the record
 * publishes. Written once, before the record; a record write lost with the
 * sender's process is completed from it by any instance's sweep.
 */
export interface NativeEvidence {
	callId: string;
	tenantId: string;
	dispatchId: string;
	routeId: string;
	capturedAt: string;
	request: { method: string; url: string; contentType: string; body: string; bodyTruncated: boolean };
	response?: {
		status: number;
		headers: Record<string, string>;
		bodyBase64: string;
		bodyBytes: number;
		bodyTruncated: boolean;
	};
	transportError?: string;
	settlement: Settlement;
}

/** The pending marker of a call: written before its record, so the sweep discovers the call however early its opener died. */
export interface PendingMarker {
	/** The call's deadline; an orphan marker (no record) is cleared only after it plus the reclaim grace. */
	deadline?: string;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

type Kind = "calls" | "evidence" | "pending" | "cancel";

function keyOf(kind: Kind, scope: CallScope): string {
	return `${kind}/${scope.callId}/${scope.tenantId}`;
}

/** The scope a key of the kind addresses; undefined for a key of another layout (never acted on). */
function scopeOfKey(kind: Kind, key: string): CallScope | undefined {
	const parts = key.split("/");
	if (parts.length !== 3 || parts[0] !== kind || !parts[1] || !parts[2]) return undefined;
	return { callId: parts[1], tenantId: parts[2] };
}

/** The evidence key of a scope: what a record's evidenceRef names once the record took the settlement from it. */
export function evidenceKeyOf(scope: CallScope): string {
	return keyOf("evidence", scope);
}

/** Typed access to the objects of the Proxy, every one addressed by the call's scope. */
export class CallStore {
	constructor(readonly objects: ObjectStore) {}

	async read(scope: CallScope): Promise<{ record: CallRecord; version: string } | undefined> {
		const o = await this.objects.get(keyOf("calls", scope));
		if (!o) return undefined;
		return { record: JSON.parse(decoder.decode(o.body)) as CallRecord, version: o.version };
	}

	/** Creates the record; PreconditionFailed when the call is already recorded under its scope. */
	async create(record: CallRecord): Promise<string> {
		return this.objects.put(keyOf("calls", record), encoder.encode(JSON.stringify(record)), { ifNoneMatch: true });
	}

	/** Replaces the record read at `version`; PreconditionFailed when another writer moved it. */
	async update(record: CallRecord, version: string): Promise<string> {
		return this.objects.put(keyOf("calls", record), encoder.encode(JSON.stringify(record)), { ifMatch: version });
	}

	/** The scopes recorded under a call id (one per tenant that used it): what a query by call id alone must choose from. */
	async listCalls(callId: string): Promise<CallScope[]> {
		const out: CallScope[] = [];
		for (const key of await this.objects.list(`calls/${callId}/`)) {
			const scope = scopeOfKey("calls", key);
			if (scope) out.push(scope);
		}
		return out;
	}

	async writeEvidence(evidence: NativeEvidence): Promise<string> {
		const key = evidenceKeyOf(evidence);
		try {
			await this.objects.put(key, encoder.encode(JSON.stringify(evidence)), { ifNoneMatch: true });
		} catch (err) {
			// Evidence is written once per call; a repeated finalization keeps the first.
			if (!(err instanceof PreconditionFailed)) throw err;
		}
		return key;
	}

	async readEvidence(scope: CallScope): Promise<NativeEvidence | undefined> {
		const o = await this.objects.get(evidenceKeyOf(scope));
		return o ? (JSON.parse(decoder.decode(o.body)) as NativeEvidence) : undefined;
	}

	/** Creates the pending marker if absent (an existing marker is left as it is). */
	async markPending(scope: CallScope, deadline: string): Promise<void> {
		const marker: PendingMarker = { deadline };
		try {
			await this.objects.put(keyOf("pending", scope), encoder.encode(JSON.stringify(marker)), { ifNoneMatch: true });
		} catch (err) {
			if (!(err instanceof PreconditionFailed)) throw err;
		}
	}

	async readPending(scope: CallScope): Promise<PendingMarker | undefined> {
		const o = await this.objects.get(keyOf("pending", scope));
		if (!o) return undefined;
		try {
			const parsed = JSON.parse(decoder.decode(o.body)) as PendingMarker;
			return typeof parsed === "object" && parsed !== null ? parsed : {};
		} catch {
			return {};
		}
	}

	async clearPending(scope: CallScope): Promise<void> {
		await this.objects.delete(keyOf("pending", scope));
	}

	async listPending(): Promise<CallScope[]> {
		const out: CallScope[] = [];
		for (const key of await this.objects.list("pending/")) {
			const scope = scopeOfKey("pending", key);
			if (scope) out.push(scope);
		}
		return out;
	}

	async requestCancel(scope: CallScope, at: string): Promise<void> {
		try {
			await this.objects.put(keyOf("cancel", scope), encoder.encode(JSON.stringify({ requestedAt: at })), {
				ifNoneMatch: true,
			});
		} catch (err) {
			if (!(err instanceof PreconditionFailed)) throw err;
		}
	}

	async cancelRequested(scope: CallScope): Promise<string | undefined> {
		const o = await this.objects.get(keyOf("cancel", scope));
		if (!o) return undefined;
		return (JSON.parse(decoder.decode(o.body)) as { requestedAt: string }).requestedAt;
	}
}
