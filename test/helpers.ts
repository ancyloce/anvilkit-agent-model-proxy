import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { type Config, loadFrom } from "../src/config.js";

export const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

export function scratch(prefix = "anvilkit-model-proxy-"): string {
	return mkdtempSync(path.join(tmpdir(), prefix));
}

/** Writes a configuration file in a scratch directory and loads it with the given environment. */
export function configFrom(yaml: string, environ: Record<string, string | undefined> = {}): Config {
	const dir = scratch();
	const file = path.join(dir, "config.yaml");
	writeFileSync(file, yaml);
	return loadFrom(file, environ);
}

/** The checked-in config.yaml text. */
export function checkedInConfig(): string {
	return readFileSync(path.join(packageRoot, "config.yaml"), "utf8");
}
