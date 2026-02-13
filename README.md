# Apollo Filter App (Frontend + Backend + Postgres)

This repository is initialized for a full-stack app with:

- `frontend`: React + Vite + TypeScript
- `backend`: Express + TypeScript + Postgres client (`pg`)
- `docker-compose`: frontend, backend, postgres services
- `Makefile`: install, lint, build, test, and container lifecycle targets

## Why pnpm

`pnpm` is used for fast installs, deterministic lockfiles, and disk-efficient package storage.

## Prerequisites

- Node.js 20+
- `pnpm` (or Corepack-enabled Node)
- Docker Desktop

## Quick Start (Local)

```bash
make install
make lint
make build
make test
```

Run app locally in two terminals:

```bash
make dev-backend
make dev-frontend
```

- Frontend: http://localhost:5173
- Backend: http://localhost:3000/health

## Docker Compose

```bash
make up
```

Then open:

- Frontend: http://localhost:8080
- Backend health: http://localhost:3000/health
- DB health: http://localhost:3000/db/health

Stop:

```bash
make down
```

Remove volumes too:

```bash
make clean
```

## Frontend Best-Practice Baseline

- Strict TypeScript in UI code
- ESLint with React Hooks and React Refresh rules
- Vitest + Testing Library for component tests
- API access via `/api` proxy to avoid hardcoding hosts in frontend code
- Keep business logic in reusable hooks/services as app grows
