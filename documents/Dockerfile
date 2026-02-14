# NOTE: Can be moved into another image for caching.
FROM python:3.14-slim-bookworm as base

# Install ffmpeg for audio, libreoffice for conversion, pandoc for documents.
RUN apt-get update \
    && (for i in $(seq 1 4); do apt-get install --no-install-recommends -y \
    default-jre ffmpeg libreoffice-java-common libreoffice*nogui pandoc \
    && break || sleep 3; done) \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

########################
########################

# This stage only downloads the dependencies into virtualenv, for caching.
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder
WORKDIR /app

# Define a persistent location for NLTK data.
ENV NLTK_DATA=/app/nltk_data

# First, copy only the files needed for dependency resolution.
# This allows Docker to cache the dependency installation layer.
COPY pyproject.toml uv.lock /app/
COPY documents/pyproject.toml /app/documents/
COPY base/pyproject.toml /app/base/

# Install dependencies and download required NLTK data.
RUN uv sync --frozen --no-install-project --no-dev --package nandam-documents
RUN .venv/bin/python -m nltk.downloader -d ${NLTK_DATA} \
    averaged_perceptron_tagger averaged_perceptron_tagger_eng punkt punkt_tab

########################
########################

# This stage builds the final runtime environment, keeping only what is
# absolutely necessary to run the service.
FROM base AS runtime

ENV HOME=/home/service
ENV NLTK_DATA=/app/nltk_data
ENV PATH="/app/.venv/bin:$PATH"
ENV PORT=8010
ENV VIRTUAL_ENV=/app/.venv

# Create non-root user and group to run the application securely.
RUN addgroup --system --gid 1000 service && \
    adduser --system --uid 1000 --gid 1000 --shell /bin/sh --no-create-home service && \
    mkdir /home/service && \
    chown -R service:service /home/service

# Copy the virtual environment, NLTK data, and application code from the builder stage.
# Set ownership to the non-root user `service` for security.
COPY --from=builder --chown=service:service /app/.venv /app/.venv
COPY --from=builder --chown=service:service ${NLTK_DATA} ${NLTK_DATA}
COPY --chown=service:service base /app/base
COPY --chown=service:service documents /app/documents

# Switch to the non-root user.
USER service
WORKDIR /app

# Set the command to run the application.
CMD ["/app/.venv/bin/python", "-m", "documents.app"]
