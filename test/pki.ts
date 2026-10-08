// Throwaway CAs and leaves for the identity tests (openssl in a temporary
// directory): no Kubernetes, no cert-manager, nothing outside the test.
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

export interface CA {
	dir: string;
	pem: Buffer;
}

export interface Leaf {
	certPem: Buffer;
	keyPem: Buffer;
}

export function spiffe(trustDomain: string, namespace: string, sa: string): string {
	return `spiffe://${trustDomain}/ns/${namespace}/sa/${sa}`;
}

export function newCA(name: string): CA {
	const dir = mkdtempSync(path.join(tmpdir(), "anvilkit-ca-"));
	execFileSync("openssl", [
		"req",
		"-x509",
		"-newkey",
		"ec",
		"-pkeyopt",
		"ec_paramgen_curve:prime256v1",
		"-nodes",
		"-keyout",
		path.join(dir, "ca.key"),
		"-out",
		path.join(dir, "ca.crt"),
		"-days",
		"2",
		"-subj",
		`/CN=${name}`,
		"-addext",
		"basicConstraints=critical,CA:TRUE",
		"-addext",
		"keyUsage=critical,keyCertSign,cRLSign",
	]);
	return { dir, pem: readFileSync(path.join(dir, "ca.crt")) };
}

/** Issues a leaf with the given URI SANs (none for a certificate without a workload identity), localhost and 127.0.0.1. */
export function issue(ca: CA, cn: string, uris: string[], dnsNames: string[] = [], days = 1): Leaf {
	const out = mkdtempSync(path.join(tmpdir(), "anvilkit-leaf-"));
	const san = [
		...uris.map((u) => `URI:${u}`),
		"IP:127.0.0.1",
		"DNS:localhost",
		...dnsNames.map((d) => `DNS:${d}`),
	].join(",");
	execFileSync("openssl", [
		"req",
		"-new",
		"-newkey",
		"ec",
		"-pkeyopt",
		"ec_paramgen_curve:prime256v1",
		"-nodes",
		"-keyout",
		path.join(out, "tls.key"),
		"-out",
		path.join(out, "req.csr"),
		"-subj",
		`/CN=${cn}`,
	]);
	writeFileSync(
		path.join(out, "ext.cnf"),
		`subjectAltName=${san}\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=critical,digitalSignature\nbasicConstraints=CA:FALSE\n`,
	);
	execFileSync("openssl", [
		"x509",
		"-req",
		"-in",
		path.join(out, "req.csr"),
		"-CA",
		path.join(ca.dir, "ca.crt"),
		"-CAkey",
		path.join(ca.dir, "ca.key"),
		"-CAcreateserial",
		"-out",
		path.join(out, "tls.crt"),
		"-days",
		String(days),
		"-extfile",
		path.join(out, "ext.cnf"),
	]);
	const leaf = { certPem: readFileSync(path.join(out, "tls.crt")), keyPem: readFileSync(path.join(out, "tls.key")) };
	rmSync(out, { recursive: true, force: true });
	return leaf;
}

/**
 * Writes tls.crt, tls.key and ca.crt into dir the way the kubelet mounts a
 * Secret: a timestamped directory, a ..data symlink swapped atomically and
 * stable names that are symlinks into ..data. Calling it again rotates.
 */
export function mount(dir: string, leaf: Leaf, caBundle: Buffer): void {
	mkdirSync(dir, { recursive: true });
	const ts = mkdtempSync(path.join(dir, "..ts-"));
	writeFileSync(path.join(ts, "tls.crt"), leaf.certPem);
	writeFileSync(path.join(ts, "tls.key"), leaf.keyPem, { mode: 0o600 });
	writeFileSync(path.join(ts, "ca.crt"), caBundle);
	const tmp = path.join(dir, "..data_tmp");
	rmSync(tmp, { force: true });
	symlinkSync(path.basename(ts), tmp);
	renameSync(tmp, path.join(dir, "..data"));
	for (const name of ["tls.crt", "tls.key", "ca.crt"]) {
		const p = path.join(dir, name);
		try {
			symlinkSync(path.join("..data", name), p);
		} catch {
			// already linked
		}
	}
}

export function files(dir: string): { certFile: string; keyFile: string; caFile: string } {
	return { certFile: path.join(dir, "tls.crt"), keyFile: path.join(dir, "tls.key"), caFile: path.join(dir, "ca.crt") };
}

export function tempDir(): string {
	return mkdtempSync(path.join(tmpdir(), "anvilkit-identity-"));
}
