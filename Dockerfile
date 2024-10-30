# Build stage
FROM denoland/deno:2.0.0 AS builder

WORKDIR /app

COPY . /app

RUN deno cache server.ts && \
    deno install --allow-scripts && \
    deno compile --allow-net --allow-read --allow-env --unstable-kv --no-check server.ts

# Runtime stage
FROM denoland/deno:2.0.0

WORKDIR /app

COPY --from=builder /app/server /app/server

# Create a non-root user if it doesn't exist
RUN if ! getent group deno > /dev/null 2>&1; then \
        addgroup --system deno; \
    fi && \
    if ! getent passwd deno > /dev/null 2>&1; then \
        adduser --system --ingroup deno deno; \
    fi

# Set environment variables
ENV DENO_DIR=/deno-dir
ENV DENO_INSTALL_ROOT=/usr/local
ENV PORT=8000

# Expose the port the app runs on
EXPOSE $PORT

# Use the non-root user
USER deno

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:$PORT/ || exit 1

# Run the compiled binary
CMD ["./server"]