FROM node:22.23.5-bookworm-slim AS base
WORKDIR /app
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends ca-certificates openssl && rm -rf /var/lib/apt/lists/* \
    && npm install --global pnpm@9.15.9

FROM base AS build
COPY . .
# No dependency resolution during release builds: require the committed lockfile.
RUN test -f pnpm-lock.yaml || (echo 'Generate and commit pnpm-lock.yaml: pnpm deps:lock' >&2; exit 1)
RUN pnpm install --frozen-lockfile
RUN pnpm build

FROM base AS runtime
ENV NODE_ENV=production
RUN groupadd --gid 10001 cajeerbots && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /app cajeerbots \
    && mkdir -p /var/lib/cajeerbots/storage /var/lib/cajeerbots/extensions \
    && chown -R 10001:10001 /app /var/lib/cajeerbots
# Keep schema, migrations, docs and contracts used by the existing maintenance jobs.
COPY --from=build --chown=10001:10001 /app/ /app/
USER 10001:10001
CMD ["node", "apps/worker/dist/main.js"]
