// The production build: one ESM entry under dist/ with the generated contract
// consumers (@anvilkit/generated-clients, TypeScript sources of the pinned
// contracts commit) compiled in and every other package left external, so
// the runtime resolves the locked node_modules of this package and nothing
// outside it. Type checking is tsc's (package.json "build").
import { build } from "esbuild";

await build({
	entryPoints: ["src/main.ts"],
	bundle: true,
	platform: "node",
	format: "esm",
	target: "node24",
	outdir: "dist",
	sourcemap: false,
	logLevel: "warning",
	plugins: [
		{
			name: "externals",
			setup(b) {
				b.onResolve({ filter: /^[^./#]/ }, (args) =>
					args.path.startsWith("@anvilkit/") ? undefined : { path: args.path, external: true },
				);
			},
		},
	],
});
