# Multi-stage build

# Stage 1 - Builder
FROM python:3.12.1-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

COPY pyproject.toml .

RUN uv venv /opt/venv && \
    . /opt/venv/bin/activate && \
    uv pip install --no-cache -e .

# Stage 1.5 - Go Agent Builder
FROM golang:1.22-alpine AS agent-builder
WORKDIR /agent
COPY agent/go.mod agent/go.sum ./
RUN go mod download
COPY agent/ .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /orchestrator-agent .

# Stage 2 - Development
FROM python:3.12.1-slim AS development

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    git \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz | \
    tar xz --strip-components=1 -C /usr/local/bin docker/docker && \
    chmod +x /usr/local/bin/docker

RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && \
    rm kubectl

RUN curl -Lo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/v0.27.0/kind-linux-amd64" && \
    chmod +x /usr/local/bin/kind

# Deployment tools: helm, skaffold, kustomize, linkerd
RUN curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

RUN curl -Lo /usr/local/bin/skaffold "https://storage.googleapis.com/skaffold/releases/latest/skaffold-linux-amd64" && \
    chmod +x /usr/local/bin/skaffold

RUN curl -s "https://raw.githubusercontent.com/kubernetes-sigs/kustomize/master/hack/install_kustomize.sh" | bash && \
    mv kustomize /usr/local/bin/

RUN curl -fsL https://run.linkerd.io/install | sh && \
    cp -L /root/.linkerd2/bin/linkerd /usr/local/bin/linkerd && \
    chmod 755 /usr/local/bin/linkerd

COPY --from=builder /opt/venv /opt/venv
COPY --from=agent-builder /orchestrator-agent /app/agent/orchestrator-agent
ENV PATH="/opt/venv/bin:/usr/local/bin:/usr/bin:/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

COPY pyproject.toml ./

ENV PATH="/root/.local/bin:$PATH"
RUN . /opt/venv/bin/activate && \
    uv pip install --no-cache -e ".[dev]"

COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# Stage 3 - Production
FROM python:3.12-slim AS production

RUN groupadd -r appuser && \
    useradd -r -g appuser -u 1001 appuser

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    git \
    netcat-openbsd \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz | \
    tar xz --strip-components=1 -C /usr/local/bin docker/docker && \
    chmod +x /usr/local/bin/docker

RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && \
    rm kubectl

RUN curl -Lo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/v0.27.0/kind-linux-amd64" && \
    chmod +x /usr/local/bin/kind

# Deployment tools: helm, skaffold, kustomize, linkerd
RUN curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

RUN curl -Lo /usr/local/bin/skaffold "https://storage.googleapis.com/skaffold/releases/latest/skaffold-linux-amd64" && \
    chmod +x /usr/local/bin/skaffold

RUN curl -s "https://raw.githubusercontent.com/kubernetes-sigs/kustomize/master/hack/install_kustomize.sh" | bash && \
    mv kustomize /usr/local/bin/

RUN curl -fsL https://run.linkerd.io/install | sh && \
    cp -L /root/.linkerd2/bin/linkerd /usr/local/bin/linkerd && \
    chmod 755 /usr/local/bin/linkerd

COPY --from=builder /opt/venv /opt/venv
COPY --from=agent-builder /orchestrator-agent /app/agent/orchestrator-agent
ENV PATH="/opt/venv/bin:/usr/local/bin:/usr/bin:/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

COPY --chown=appuser:appuser app/ ./app/
COPY --chown=appuser:appuser pipeline/ ./pipeline/
COPY --chown=appuser:appuser pyproject.toml ./
COPY --chown=appuser:appuser scripts/entrypoint.sh ./

RUN chmod +x entrypoint.sh

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

ENTRYPOINT ["./entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
