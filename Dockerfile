FROM python:3.14-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ARG UID=1000
ARG GID=1000

RUN groupadd -g $GID vscode || true && \
    useradd -l -u $UID -g $GID -m -s /bin/bash vscode || true

RUN chown vscode:vscode /app

USER vscode

ENV PATH="/home/vscode/.local/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY --chown=vscode:vscode requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt
RUN pip install --user ruff

COPY --chown=vscode:vscode . .

EXPOSE 8000
