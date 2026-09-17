import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		include: ["test/**/*.test.ts"],
		testTimeout: 120_000,
		hookTimeout: 120_000,
		// The scenarios bind loopback listeners (an upstream double, a fake
		// Control, the Proxy itself) and share the process-wide guarded fetch.
		fileParallelism: false,
		server: {
			deps: {
				// The generated contract consumers are TypeScript sources of a
				// git-hosted package; Node refuses to strip types under
				// node_modules, so Vitest transforms them (the build bundles them).
				inline: [/@anvilkit\/generated-clients/],
			},
		},
	},
});
