// The workload identity of this process's gRPC clients (P0.1, security.md
// SEC-01): a validating file watcher that feeds grpc-js's certificate
// provider channel credentials, so every new connection presents the
// current workload certificate and verifies the server against the current
// CA bundle with standard hostname verification by the reviewed server
// name. An invalid update never becomes active material. This is the
// per-repository copy of the client half of the Knowledge module.
import { createHash, createPrivateKey, X509Certificate } from "node:crypto";
import { readFileSync } from "node:fs";
import { type ChannelCredentials, type ChannelOptions, experimental } from "@grpc/grpc-js";

export interface Files {
	certFile: string;
	keyFile: string;
	caFile: string;
}

/** One complete, validated identity configuration; immutable. */
export interface Material {
	certPem: Buffer;
	keyPem: Buffer;
	caPem: Buffer;
	leaf: X509Certificate;
	cas: X509Certificate[];
	digest: string;
}

/** Validates the material as a whole: the key must match the certificate, the leaf must be valid now, the bundle must hold CA certificates only. */
export function parseMaterial(certPem: Buffer, keyPem: Buffer, caPem: Buffer, now = new Date()): Material {
	let leaf: X509Certificate;
	try {
		leaf = new X509Certificate(certPem);
	} catch (err) {
		throw new Error(`leaf: ${err instanceof Error ? err.message : String(err)}`);
	}
	let key: ReturnType<typeof createPrivateKey>;
	try {
		key = createPrivateKey(keyPem);
	} catch (err) {
		throw new Error(`key: ${err instanceof Error ? err.message : String(err)}`);
	}
	if (!leaf.checkPrivateKey(key)) throw new Error("certificate and key: key values mismatch");
	if (now < new Date(leaf.validFrom) || now > new Date(leaf.validTo))
		throw new Error(`leaf is not valid at ${now.toISOString()} (valid ${leaf.validFrom} to ${leaf.validTo})`);
	const cas: X509Certificate[] = [];
	for (const block of caPem.toString("utf8").split(/(?=-----BEGIN CERTIFICATE-----)/)) {
		if (!block.includes("-----BEGIN CERTIFICATE-----")) continue;
		let ca: X509Certificate;
		try {
			ca = new X509Certificate(block.trim());
		} catch (err) {
			throw new Error(`ca bundle: ${err instanceof Error ? err.message : String(err)}`);
		}
		if (!ca.ca) throw new Error(`ca bundle holds a non-CA certificate (${ca.subject})`);
		cas.push(ca);
	}
	if (cas.length === 0) throw new Error("ca bundle holds no certificate");
	const h = createHash("sha256");
	for (const b of [certPem, keyPem, caPem]) {
		h.update(b);
		h.update(Buffer.from([0]));
	}
	return { certPem, keyPem, caPem, leaf, cas, digest: h.digest("hex") };
}

export function loadMaterial(f: Files, now = new Date()): Material {
	return parseMaterial(readFileSync(f.certFile), readFileSync(f.keyFile), readFileSync(f.caFile), now);
}

/**
 * IdentityWatcher keeps the current Material of one set of files, polls
 * them for changes (mounted Secrets swap a ..data symlink; the files are
 * read by path every time) and feeds grpc-js's certificate-provider
 * credentials. A changed set is validated as a whole and published
 * atomically; an invalid candidate (a file missing mid-swap, a key that
 * does not match, an expired leaf, an empty bundle) is counted, logged and
 * leaves the current material in force. There is no fallback: construction
 * refuses invalid material.
 */
export class IdentityWatcher implements experimental.CertificateProvider {
	private material: Material;
	private timer?: NodeJS.Timeout;
	private readonly caListeners = new Set<experimental.CaCertificateUpdateListener>();
	private readonly identityListeners = new Set<experimental.IdentityCertificateUpdateListener>();
	private readonly changeListeners = new Set<(previous: Material, current: Material) => void>();
	private rejected = 0;

	constructor(
		private readonly files: Files,
		private readonly intervalMs = 5000,
		private readonly log: { warn(msg: string, f?: Record<string, string>): void } = { warn: () => {} },
	) {
		this.material = loadMaterial(files);
	}

	current(): Material {
		return this.material;
	}

	/** Rejected candidates since the last accepted one: the failure signal. */
	failures(): number {
		return this.rejected;
	}

	start(): void {
		if (this.timer) return;
		this.timer = setInterval(() => this.reload(), this.intervalMs);
		this.timer.unref();
	}

	stop(): void {
		if (this.timer) clearInterval(this.timer);
		this.timer = undefined;
	}

	onChange(fn: (previous: Material, current: Material) => void): void {
		this.changeListeners.add(fn);
	}

	/** Checks the files once; returns whether a new material was published. */
	reload(): boolean {
		let candidate: Material;
		try {
			const cert = readFileSync(this.files.certFile);
			const key = readFileSync(this.files.keyFile);
			const ca = readFileSync(this.files.caFile);
			const h = createHash("sha256");
			for (const b of [cert, key, ca]) {
				h.update(b);
				h.update(Buffer.from([0]));
			}
			if (h.digest("hex") === this.material.digest) return false;
			candidate = parseMaterial(cert, key, ca);
		} catch (err) {
			this.rejected++;
			this.log.warn("identity reload rejected; current material kept", {
				error: err instanceof Error ? err.message : String(err),
			});
			return false;
		}
		const previous = this.material;
		this.material = candidate;
		this.rejected = 0;
		this.publish();
		for (const fn of this.changeListeners) fn(previous, candidate);
		return true;
	}

	private publish(): void {
		for (const l of this.caListeners) l({ caCertificate: this.material.caPem });
		for (const l of this.identityListeners) l({ certificate: this.material.certPem, privateKey: this.material.keyPem });
	}

	// The initial update is delivered asynchronously: grpc-js subscribes to
	// the provider before it registers its own secure-context watcher, so a
	// synchronous callback would be published to nobody.
	addCaCertificateListener(listener: experimental.CaCertificateUpdateListener): void {
		this.caListeners.add(listener);
		queueMicrotask(() => {
			if (this.caListeners.has(listener)) listener({ caCertificate: this.material.caPem });
		});
	}

	removeCaCertificateListener(listener: experimental.CaCertificateUpdateListener): void {
		this.caListeners.delete(listener);
	}

	addIdentityCertificateListener(listener: experimental.IdentityCertificateUpdateListener): void {
		this.identityListeners.add(listener);
		queueMicrotask(() => {
			if (this.identityListeners.has(listener))
				listener({ certificate: this.material.certPem, privateKey: this.material.keyPem });
		});
	}

	removeIdentityCertificateListener(listener: experimental.IdentityCertificateUpdateListener): void {
		this.identityListeners.delete(listener);
	}
}

/** Channel credentials presenting the watcher's current leaf and verifying the server against its current bundle. */
export function clientCredentials(watcher: IdentityWatcher): ChannelCredentials {
	return experimental.createCertificateProviderChannelCredentials(watcher, watcher, {});
}

/** The channel options of a client: standard hostname verification against the reviewed server name (a DNS SAN). */
export function clientOptions(serverName: string): ChannelOptions {
	if (!serverName) throw new Error("identity: server_name is required for hostname verification");
	return { "grpc.ssl_target_name_override": serverName };
}
