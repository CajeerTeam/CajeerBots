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

# Add only required maintenance tools, from authenticated Debian packages.
FROM toolchain AS runtime-support
RUN set -eux; apt-get update; mkdir -p /runtime-root/var/lib/dpkg/status.d /debs; \
    cd /debs; apt-get download busybox-static libatomic1 openssl; \
    for deb in *.deb; do \
      pkg="$(dpkg-deb -f "$deb" Package)"; \
      dpkg-deb -x "$deb" /runtime-root; \
      { dpkg-deb -f "$deb"; printf '\n'; } > /runtime-root/var/lib/dpkg/status.d/"$pkg"; \
    done; \
    test -x /runtime-root/usr/bin/busybox; \
    for applet in sh tar uname ls grep cat; do ln -s busybox /runtime-root/usr/bin/"$applet"; done; \
    mkdir -p /runtime-root/etc /runtime-data/storage /runtime-data/extensions; \
    printf 'root:x:0:0:root:/root:/bin/sh\ncajeerbots:x:10001:10001:CajeerBots:/var/lib/cajeerbots:/bin/sh\nnobody:x:65534:65534:nobody:/nonexistent:/bin/false\n' > /runtime-root/etc/passwd; \
    printf 'root:x:0:\ncajeerbots:x:10001:\nnogroup:x:65534:\n' > /runtime-root/etc/group

# The upstream Distroless package database remains intact; no CVE exclusions.
FROM gcr.io/distroless/cc-debian13:latest AS runtime
WORKDIR /app
ENV NODE_ENV=production HOME=/var/lib/cajeerbots
COPY --from=runtime-support /runtime-root/ /
COPY --from=runtime-support --chown=10001:10001 /runtime-data/ /var/lib/cajeerbots/
COPY --from=toolchain /usr/local/bin/node /usr/local/bin/node
COPY --from=production-deps --chown=10001:10001 /app/ /app/
USER 10001:10001
RUN ["/bin/sh", "-ec", "node --version && node node_modules/prisma/build/index.js --version && node scripts/rehearsal-workload.mjs --check-imports && node scripts/runtime-os-inventory.mjs && test -f prisma/seed.js && test -w /var/lib/cajeerbots/storage && ! command -v npm && ! command -v pnpm"]
CMD ["node", "apps/bot-runtime/dist/main.js"]
