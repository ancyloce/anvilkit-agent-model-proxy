# anvilkit-agent-model-proxy: built from this repository alone (the build
# context is the repository root; nothing from the parent checkout is read).
# The generated contract consumers (@anvilkit/generated-clients) are a
# git-hosted dependency of the contracts repository pinned by commit and
# integrity in pnpm-lock.yaml; the contract document the Proxy validates
# requests against (openapi/model-proxy.yaml, the same file the Go consumers
# embed) comes from the contracts repository as a named build context, for
# example a checkout of the pinned commit (package.json, .github/workflows/ci.yml):
#   docker build --build-context contracts=../../../contracts -t anvilkit-agent-model-proxy .
# The image takes only the document of that pinned commit: its digest is
# checked below, so a context at another revision fails the build instead of
# shipping a schema the bundled consumers were not generated from.
# No lifecycle script runs in the installs (pnpm-workspace.yaml).
FROM node:24.19.0-bookworm-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df AS build
WORKDIR /src
RUN npm install -g pnpm@12.3.4
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json build.mjs ./
RUN pnpm install --frozen-lockfile --ignore-scripts
COPY src ./src
RUN pnpm run build
# The runtime dependencies alone (the bundle carries the contract consumers).
RUN pnpm install --frozen-lockfile --ignore-scripts --prod

# Runtime: the bundle, its locked production dependencies, the reviewed
# secret-free configuration file, the contract document and a non-root
# user. The listener, the Control placement, the principals, the store and
# the route credentials are supplied through the allowlisted
# ANVILKIT_MODEL_PROXY_* environment; the file itself may be replaced by
# mounting one at the path named by ANVILKIT_MODEL_PROXY_CONFIG.
FROM node:24.19.0-bookworm-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df
WORKDIR /anvilkit/model-proxy
COPY --from=build /src/node_modules ./node_modules
COPY --from=build /src/dist ./dist
COPY package.json ./package.json
COPY config.yaml /etc/anvilkit/anvilkit-agent-model-proxy/config.yaml
COPY --from=contracts openapi/model-proxy.yaml /anvilkit/contracts/openapi/model-proxy.yaml
# openapi/model-proxy.yaml of contracts commit 1e7cb5d5c53752c2337af1655ee5c2d20212c47e
# (line endings normalized: a CRLF checkout carries the same document).
ARG CONTRACT_SHA256=d2d686d11ea503485b8ffa89cb7d4a738c40f02a0435be935dd5f04a3b453450
RUN test "$(tr -d '\r' < /anvilkit/contracts/openapi/model-proxy.yaml | sha256sum | cut -d' ' -f1)" = "$CONTRACT_SHA256" \
 || { echo "openapi/model-proxy.yaml is not the document of the pinned contracts commit" >&2; exit 1; }
ENV ANVILKIT_MODEL_PROXY_CONFIG=/etc/anvilkit/anvilkit-agent-model-proxy/config.yaml \
    ANVILKIT_MODEL_PROXY_CONTRACTS_DIR=/anvilkit/contracts \
    NODE_ENV=production
USER 65532:65532
ENTRYPOINT ["node", "/anvilkit/model-proxy/dist/main.js"]
