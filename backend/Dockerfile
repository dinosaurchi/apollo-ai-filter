FROM node:20-alpine AS build
WORKDIR /app
RUN corepack enable

COPY package.json ./
RUN pnpm install

COPY tsconfig.json ./
COPY tsconfig.build.json ./
COPY eslint.config.mjs ./
COPY vitest.config.ts ./
COPY src ./src

RUN pnpm build

FROM node:20-alpine AS runtime
WORKDIR /app
RUN corepack enable

COPY package.json ./
RUN pnpm install --prod

COPY --from=build /app/dist ./dist

ENV NODE_ENV=production
EXPOSE 3000

CMD ["pnpm", "start"]
