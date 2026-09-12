FROM node:lts-alpine AS base
WORKDIR /app
RUN corepack enable

FROM base AS deps
COPY package.json pnpm-workspace.yaml ./
COPY apps ./apps
COPY packages ./packages
COPY prisma ./prisma
RUN pnpm install --frozen-lockfile=false

FROM deps AS build
COPY . .
RUN pnpm prisma:generate && pnpm build

FROM node:lts-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
RUN corepack enable && addgroup -S -g 10001 cajeerbots && adduser -S -D -H -u 10001 -G cajeerbots cajeerbots && mkdir -p /var/lib/cajeerbots/storage /var/lib/cajeerbots/extensions && chown -R cajeerbots:cajeerbots /app /var/lib/cajeerbots
COPY --from=build --chown=cajeerbots:cajeerbots /app/package.json /app/pnpm-workspace.yaml ./
COPY --from=build --chown=cajeerbots:cajeerbots /app/node_modules ./node_modules
COPY --from=build --chown=cajeerbots:cajeerbots /app/apps ./apps
COPY --from=build --chown=cajeerbots:cajeerbots /app/packages ./packages
COPY --from=build --chown=cajeerbots:cajeerbots /app/prisma ./prisma
USER cajeerbots
CMD ["node", "apps/bot-runtime/dist/main.js"]
