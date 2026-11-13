# Build tools never enter the runtime filesystem.
FROM node:22.23.5-trixie-slim AS toolchain
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates openssl \
    && rm -rf /var/lib/apt/lists/* && npm install --global pnpm@9.15.9
ENV CI=true

FROM toolchain AS build
COPY . .
RUN test -s pnpm-lock.yaml && pnpm install --frozen-lockfile && pnpm build
RUN pnpm exec tsc -p tsconfig.seed.json \
    && mkdir -p /compiled/prisma \
    && cp -a prisma/generated /compiled/prisma/ \
    && cp -a .tmp/seed-build/. /compiled/ \
    && find apps packages examples -name node_modules -prune -o -type d -name dist -prune -exec cp -a --parents {} /compiled/ \;

# Install a fresh, frozen production graph; do not copy the builder's node_modules.
FROM toolchain AS production-deps
ENV NODE_ENV=production
COPY . .
RUN pnpm install --prod --frozen-lockfile \
    && test -f node_modules/prisma/build/index.js \
    && test ! -e node_modules/tsx && test ! -e node_modules/vitest \
    && test -z "$(find node_modules/.pnpm -path '*/bin/esbuild' -print -quit)"
COPY --from=build /compiled/ /app/

FROM debian:trixie-slim AS runtime
WORKDIR /app
ENV NODE_ENV=production
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates openssl libstdc++6 libatomic1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 cajeerbots \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /app cajeerbots \
    && mkdir -p /var/lib/cajeerbots/storage /var/lib/cajeerbots/extensions \
    && chown -R 10001:10001 /app /var/lib/cajeerbots
COPY --from=toolchain /usr/local/bin/node /usr/local/bin/node
COPY --from=production-deps --chown=10001:10001 /app/ /app/
USER 10001:10001
RUN node --version && node node_modules/prisma/build/index.js --version \
    && node scripts/rehearsal-workload.mjs --check-imports \
    && test -f prisma/seed.js \
    && ! command -v npm && ! command -v pnpm
CMD ["node", "apps/bot-runtime/dist/main.js"]
