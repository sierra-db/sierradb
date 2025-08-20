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

# Create non-root user with home directory
RUN useradd -r -m -s /bin/false sierradb

# Create data directory and set permissions
RUN mkdir -p /app/data && chown -R sierradb:sierradb /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/sierradb /usr/local/bin/sierradb

# Change ownership and make executable
RUN chown sierradb:sierradb /usr/local/bin/sierradb

# Switch to non-root user
USER sierradb

# Expose port 9090 (default for sierradb)
EXPOSE 9090

# Set default directory for data
ENV DIR=/app/data

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/sierradb"]
