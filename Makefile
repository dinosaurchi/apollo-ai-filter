.PHONY: help install lint build test typecheck dev-backend dev-frontend docker-build up down logs clean ssh-remote deploy-remote

PNPM ?= pnpm
DOCKER_COMPOSE ?= docker compose
REMOTE_USER ?= ubuntu
REMOTE_HOST ?= 18.246.55.226
REMOTE_SSH_KEY ?= $(HOME)/.ssh/apollo-filter-app-server.pem
REMOTE_APP_DIR ?= /home/ubuntu/apollo-filter-app-new
REMOTE_SSH := ssh -i $(REMOTE_SSH_KEY) $(REMOTE_USER)@$(REMOTE_HOST)
REMOTE_RSYNC := rsync -az --delete -e "ssh -i $(REMOTE_SSH_KEY)"
REMOTE_RSYNC_PROGRESS := rsync -az --progress -h -e "ssh -i $(REMOTE_SSH_KEY)"
REMOTE_IMAGES := apollo-filter-app-new-backend apollo-filter-app-new-frontend apollo-filter-app-new-opencode
LOCAL_DEPLOY_DIR ?= ./.deploy
REMOTE_IMAGE_DIR ?= $(REMOTE_APP_DIR)/.deploy-images

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
	@echo "  ssh-remote   SSH into the configured remote host"
	@echo "  deploy-remote Build local images and deploy to remote host via ssh/rsync"

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

# Remote server
ssh-remote:
	$(REMOTE_SSH)

deploy-remote:
	@set -euo pipefail; \
	echo "Building local images (backend/frontend/opencode) for linux/amd64..."; \
	DOCKER_DEFAULT_PLATFORM=linux/amd64 $(DOCKER_COMPOSE) build backend frontend opencode; \
	echo "Preparing remote directory $(REMOTE_APP_DIR)..."; \
	$(REMOTE_SSH) "mkdir -p '$(REMOTE_APP_DIR)/backend' '$(REMOTE_APP_DIR)/backend/ai-workspace' '$(REMOTE_IMAGE_DIR)' '$$HOME/.config/opencode' '$$HOME/.local/share/opencode' '$$HOME/.local/state/opencode' && chown -R '$(REMOTE_USER):$(REMOTE_USER)' '$$HOME/.config/opencode' '$$HOME/.local/share/opencode' '$$HOME/.local/state/opencode'"; \
	echo "Syncing docker-compose and env files..."; \
	$(REMOTE_RSYNC) docker-compose.yml "$(REMOTE_USER)@$(REMOTE_HOST):$(REMOTE_APP_DIR)/docker-compose.yml"; \
	$(REMOTE_RSYNC) backend/.env "$(REMOTE_USER)@$(REMOTE_HOST):$(REMOTE_APP_DIR)/backend/.env"; \
	mkdir -p "$(LOCAL_DEPLOY_DIR)"; \
	for image in $(REMOTE_IMAGES); do \
		local_archive="$(LOCAL_DEPLOY_DIR)/$${image}.tar.gz"; \
		remote_archive="$(REMOTE_IMAGE_DIR)/$${image}.tar.gz"; \
		docker image inspect "$$image" >/dev/null; \
		echo "Exporting $$image to $$local_archive..."; \
		docker save "$$image" | gzip -1 > "$$local_archive"; \
		if [ "$$(stat -f%z "$$local_archive")" -lt 1024 ]; then \
			echo "Archive $$local_archive is unexpectedly small; aborting."; \
			exit 1; \
		fi; \
		echo "Uploading $$image with rsync progress..."; \
		$(REMOTE_RSYNC_PROGRESS) "$$local_archive" "$(REMOTE_USER)@$(REMOTE_HOST):$$remote_archive"; \
		echo "Loading $$image on remote host..."; \
		$(REMOTE_SSH) "docker load -i '$$remote_archive' && rm -f '$$remote_archive'"; \
		rm -f "$$local_archive"; \
	done; \
	echo "Starting services on remote host..."; \
	$(REMOTE_SSH) "cd '$(REMOTE_APP_DIR)' && FRONTEND_PORT=80 docker compose up -d --remove-orphans"; \
	echo "Remote deployment complete. Current remote services:"; \
	$(REMOTE_SSH) "cd '$(REMOTE_APP_DIR)' && docker compose ps"
