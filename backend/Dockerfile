FROM node:20-alpine AS build
WORKDIR /app
RUN corepack enable

COPY package.json ./
COPY prisma ./prisma
COPY prisma.config.ts ./
RUN pnpm install

COPY tsconfig.json ./
COPY tsconfig.build.json ./
COPY eslint.config.mjs ./
COPY vitest.config.ts ./
COPY src ./src
COPY scripts ./scripts
COPY analyzer-bot ./analyzer-bot
COPY dockerfiles ./dockerfiles

RUN pnpm prisma:generate
RUN pnpm -C analyzer-bot install --no-frozen-lockfile
RUN pnpm -C analyzer-bot build
RUN pnpm build

FROM node:20-alpine AS runtime
WORKDIR /app
RUN corepack enable

COPY package.json ./
COPY prisma ./prisma
COPY prisma.config.ts ./
RUN pnpm install --prod
RUN pnpm prisma:generate

COPY --from=build /app/dist ./dist
COPY --from=build /app/analyzer-bot/package.json ./analyzer-bot/package.json
COPY --from=build /app/analyzer-bot/dist ./analyzer-bot/dist
COPY --from=build /app/analyzer-bot/contracts ./analyzer-bot/contracts
COPY --from=build /app/scripts ./scripts
COPY --from=build /app/prisma ./prisma
COPY --from=build /app/prisma.config.ts ./prisma.config.ts
RUN pnpm -C analyzer-bot install --prod --no-frozen-lockfile
COPY --from=build /app/dockerfiles ./dockerfiles

ENV NODE_ENV=production
EXPOSE 3000

CMD ["pnpm", "start"]
