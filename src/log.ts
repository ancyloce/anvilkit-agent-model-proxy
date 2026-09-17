// Structured JSON lines on stdout (A11): controlled identifiers, states,
// codes, counts and durations only. Prompts, frames, native bodies,
// credentials and tokens never reach this sink (security.md "Data
// classification, logging and deletion"); logs are diagnostic, never evidence.
export type Fields = Record<string, string | number | boolean | undefined>;

export interface Logger {
	info(msg: string, fields?: Fields): void;
	warn(msg: string, fields?: Fields): void;
	error(msg: string, fields?: Fields): void;
}

export function jsonLogger(out: (line: string) => void = (line) => process.stdout.write(`${line}\n`)): Logger {
	const emit = (level: string, msg: string, fields?: Fields) => {
		const entry: Record<string, unknown> = { time: new Date().toISOString(), level, msg };
		for (const [k, v] of Object.entries(fields ?? {})) {
			if (v !== undefined) entry[k] = v;
		}
		out(JSON.stringify(entry));
	};
	return {
		info: (m, f) => emit("INFO", m, f),
		warn: (m, f) => emit("WARN", m, f),
		error: (m, f) => emit("ERROR", m, f),
	};
}

export const silentLogger: Logger = { info: () => {}, warn: () => {}, error: () => {} };
