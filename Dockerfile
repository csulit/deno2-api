# Use the official Deno 2 image
FROM denoland/deno:2.0.0

# Set the working directory in the container
WORKDIR /app

# Copy the entire project
COPY . /app

# Install dependencies and compile the application
RUN deno cache server.ts && \
    deno compile --allow-net --allow-read --allow-env --unstable-kv server.ts

# Expose the port the app runs on
EXPOSE 8000

# Set environment variables
ENV DENO_DIR=/deno-dir
ENV DENO_INSTALL_ROOT=/usr/local

# Run the compiled binary
CMD ["./server"]