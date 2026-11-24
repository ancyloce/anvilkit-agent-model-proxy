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
# The runtime dependencies alone, installed in a directory of their own: a
# --prod install over the full one above removes nothing, and the image
# shipped every development package (vite, vitest, typescript, biome).
# The bundle carries the contract consumers.
WORKDIR /prod
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile --ignore-scripts --prod

# Runtime: the bundle, its locked production dependencies, the reviewed
# secret-free configuration file, the contract document and a non-root
# user. The listener, the Control placement, the principals, the store and
# the route credentials are supplied through the allowlisted
# ANVILKIT_MODEL_PROXY_* environment; the file itself may be replaced by
# mounting one at the path named by ANVILKIT_MODEL_PROXY_CONFIG.
FROM node:24.19.0-bookworm-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df
WORKDIR /anvilkit/model-proxy
# The runtime runs node only: the base image's npm, npx and corepack are
# removed so their bundled packages (and their advisories) are not shipped.
RUN rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack \
      /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack
# Debian security fixes the base digest predates (with a fixed version:
# CRITICAL CVE-2026-13221/42496/8376 perl-base; HIGH
# CVE-2026-86145/89157/89161/103111 libpcre2, CVE-2026-109518 apt and its
# libapt-pkg), pinned to the fixed versions; the 2026-11-23 scan adds the
# liblzma5 and tzdata point releases.
RUN apt-get update \
 && apt-get install -y --no-install-recommends --only-upgrade perl-base=5.36.0-7+deb12u4 libpcre2-8-0=10.42-1+deb12u2 \
      apt=2.6.2 libapt-pkg6.0=2.6.2 liblzma5=5.4.1-1+deb12u2 tzdata=2026f-0+deb12u1 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /prod/node_modules ./node_modules
COPY --from=build /src/dist ./dist
COPY package.json ./package.json
COPY config.yaml /etc/anvilkit/anvilkit-agent-model-proxy/config.yaml
COPY --from=contracts openapi/model-proxy.yaml /anvilkit/contracts/openapi/model-proxy.yaml
# openapi/model-proxy.yaml of contracts commit e707f7e1c712111e4ee520ceb36ace9a390bdb28 (tag go/v0.1.52, the commit
# @anvilkit/generated-clients is pinned to; ModelCall.fencedAt, DEC-068)
# (line endings normalized: a CRLF checkout carries the same document).
ARG CONTRACT_SHA256=e7de8dafcc1e885051395cc50b75fc25e9de5e0638777015f88c2e364dbf982f
RUN test "$(tr -d '\r' < /anvilkit/contracts/openapi/model-proxy.yaml | sha256sum | cut -d' ' -f1)" = "$CONTRACT_SHA256" \
 || { echo "openapi/model-proxy.yaml is not the document of the pinned contracts commit" >&2; exit 1; }
ENV ANVILKIT_MODEL_PROXY_CONFIG=/etc/anvilkit/anvilkit-agent-model-proxy/config.yaml \
    ANVILKIT_MODEL_PROXY_CONTRACTS_DIR=/anvilkit/contracts \
    NODE_ENV=production
USER 65532:65532
ENTRYPOINT ["node", "/anvilkit/model-proxy/dist/main.js"]
