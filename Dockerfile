# Multi-stage build for efficient Docker image
FROM rustlang/rust:nightly AS builder

# No additional build dependencies needed

# Set working directory
WORKDIR /app

# Copy Cargo files first for better layer caching
COPY Cargo.toml Cargo.lock ./

# Copy all workspace members' Cargo.toml files if you have a workspace
# Adjust this based on your project structure
COPY */Cargo.toml ./*/

# Copy source code
COPY . .

# Build the specific package (sierradb-server)
RUN cargo build --release --package sierradb-server

# Runtime stage with minimal base image
FROM debian:bookworm-slim AS runtime

# Install gosu for user switching
RUN apt-get update && apt-get install -y gosu && rm -rf /var/lib/apt/lists/*

# Create non-root user with home directory
RUN useradd -r -m -s /bin/false sierradb

# Create data directory and set permissions
RUN mkdir -p /app/data && chown -R sierradb:sierradb /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/sierradb /usr/local/bin/sierradb

# Create entrypoint script
RUN cat > /entrypoint.sh << 'EOF'
#!/bin/bash
# Fix permissions for mounted volumes if running as root
if [ "$(id -u)" = "0" ]; then
    chown -R sierradb:sierradb /app/data
    exec gosu sierradb "$@"
else
    exec "$@"
fi
EOF

RUN chmod +x /entrypoint.sh

# Change ownership and make executable
RUN chown sierradb:sierradb /usr/local/bin/sierradb

# Expose port 9090 (default for sierradb)
EXPOSE 9090

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh", "/usr/local/bin/sierradb"]
CMD ["--dir", "/app/data"]
