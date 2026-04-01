FROM node:22-bookworm-slim

ARG DEBIAN_FRONTEND=noninteractive
ARG USERNAME=agent
ARG UID=1000
ARG GID=1000

# Set to false to skip CLI installation at build time.
ARG INSTALL_AI_CLIS=true
ARG CODEX_NPM_PACKAGE=@openai/codex
ARG OPENCODE_NPM_PACKAGE=opencode-ai

ENV TZ=Etc/UTC \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    NPM_CONFIG_UPDATE_NOTIFIER=false \
    NPM_CONFIG_FUND=false

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    git \
    gnupg \
    jq \
    less \
    openssh-client \
    procps \
    python3 \
    python3-pip \
    python3-venv \
    tini \
    unzip \
    vim \
    wget \
    xz-utils \
    zip \
    zsh \
    && rm -rf /var/lib/apt/lists/*

RUN corepack enable

RUN if [ "${INSTALL_AI_CLIS}" = "true" ]; then \
      npm install -g "${CODEX_NPM_PACKAGE}" "${OPENCODE_NPM_PACKAGE}"; \
    fi

RUN set -eux; \
    if ! getent group "${USERNAME}" >/dev/null; then \
      if getent group "${GID}" >/dev/null; then \
        groupadd "${USERNAME}"; \
      else \
        groupadd --gid "${GID}" "${USERNAME}"; \
      fi; \
    fi; \
    if ! id -u "${USERNAME}" >/dev/null 2>&1; then \
      if getent passwd "${UID}" >/dev/null; then \
        useradd --gid "${USERNAME}" --create-home --shell /bin/bash "${USERNAME}"; \
      else \
        useradd --uid "${UID}" --gid "${USERNAME}" --create-home --shell /bin/bash "${USERNAME}"; \
      fi; \
    fi; \
    mkdir -p /workspace \
      "/home/${USERNAME}/.config" \
      "/home/${USERNAME}/.local/share" \
      "/home/${USERNAME}/.local/state" && \
    chown -R "${USERNAME}:${USERNAME}" /workspace "/home/${USERNAME}"

WORKDIR /workspace
USER ${USERNAME}

# Quick sanity defaults for agent containers.
ENV HOME=/home/${USERNAME} \
    CODEX_HOME=/home/${USERNAME}/.codex \
    PATH=/home/${USERNAME}/.local/bin:${PATH}

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/bin/bash"]
