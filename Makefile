.PHONY: help install lint build test typecheck dev-backend dev-frontend docker-build up down logs clean

PNPM ?= pnpm
DOCKER_COMPOSE ?= docker compose

help:
	@echo "Targets:"
	@echo "  install      Install backend and frontend dependencies"
	@echo "  lint         Run lint checks for backend and frontend"
	@echo "  build        Build backend and frontend"
	@echo "  test         Run tests for backend and frontend"
	@echo "  typecheck    Run TypeScript type checks"
	@echo "  dev-backend  Start backend dev server"
	@echo "  dev-frontend Start frontend dev server"
	@echo "  docker-build Build Docker images"
	@echo "  up           Start frontend, backend, postgres with docker compose"
	@echo "  down         Stop docker compose services"
	@echo "  logs         Follow docker compose logs"
	@echo "  clean        Stop services and remove volumes"

install:
	$(PNPM) -C backend install
	$(PNPM) -C frontend install

lint:
	$(PNPM) -C backend lint
	$(PNPM) -C frontend lint

build:
	$(PNPM) -C backend build
	$(PNPM) -C frontend build

test:
	$(PNPM) -C backend test
	$(PNPM) -C frontend test

typecheck:
	$(PNPM) -C backend typecheck
	$(PNPM) -C frontend typecheck

dev-backend:
	$(PNPM) -C backend dev

dev-frontend:
	$(PNPM) -C frontend dev

docker-build:
	$(DOCKER_COMPOSE) build

up:
	$(DOCKER_COMPOSE) up -d --build

down:
	$(DOCKER_COMPOSE) down

logs:
	$(DOCKER_COMPOSE) logs -f

clean:
	$(DOCKER_COMPOSE) down -v --remove-orphans
