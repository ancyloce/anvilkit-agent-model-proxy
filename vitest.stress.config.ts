import { defineConfig } from "vitest/config";
import base from "./vitest.config";

// The stress cases (P3.5): not part of `pnpm test` (a long run is not a unit
// test); `pnpm run stress` runs them alone. ANVILKIT_MODEL_PROXY_STRESS_ITERATIONS
// sets the iteration count (default 100).
export default defineConfig({
	test: { ...base.test, include: ["test/stress/**/*.stress.ts"], testTimeout: 1_800_000 },
});
