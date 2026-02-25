# Releasing SierraDB

## Prerequisites

Ensure these GitHub secrets are configured in Settings > Secrets and variables > Actions:

| Secret | Description |
|--------|-------------|
| `CARGO_REGISTRY_TOKEN` | crates.io API token |
| `DOCKERHUB_USERNAME` | Docker Hub username |
| `DOCKERHUB_TOKEN` | Docker Hub access token |

## Release Process

1. Update the version in `Cargo.toml`:

   ```toml
   [workspace.package]
   version = "X.Y.Z"
   ```

2. Commit the version bump:

   ```bash
   git commit -am "chore: bump version to X.Y.Z"
   ```

3. Create and push the tag:

   ```bash
   git tag vX.Y.Z
   git push && git push --tags
   ```

4. Monitor the release workflow in GitHub Actions.

## What Gets Published

When a `v*` tag is pushed, the CI will:

1. **Run checks** (must all pass):
   - Tests
   - Formatting (`cargo fmt --check`)
   - Clippy lints
   - Lockfile verification

2. **Publish to crates.io** (in dependency order):
   - seglog
   - sierradb-protocol
   - sierradb
   - sierradb-topology
   - sierradb-cluster
   - sierradb-client
   - sierradb-server

3. **Push Docker image** to Docker Hub:
   - `tqwewe/sierradb:latest`
   - `tqwewe/sierradb:X.Y.Z`

## Versioning

All crates share the same version defined in `workspace.package.version`. When releasing, all crates are published together regardless of which ones changed.
