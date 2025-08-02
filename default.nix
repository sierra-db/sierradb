{
  pkgs ? import <nixpkgs> { },
}:
let
  # Use nightly Rust from rust-overlay
  rustToolchain = pkgs.rust-bin.nightly.latest.default;

  # Create custom rustPlatform with nightly toolchain
  rustPlatform = pkgs.makeRustPlatform {
    cargo = rustToolchain;
    rustc = rustToolchain;
  };
in
rustPlatform.buildRustPackage {
  pname = "sierradb";
  version = "0.1";
  cargoLock = {
    lockFile = ./Cargo.lock;
    outputHashes = {
      "kameo-0.17.2" = "sha256-o7m6DZT+Q0b82xcp1PE0LeP6RZpszrESMlgKKfh4a/4=";
    };
  };
  src = pkgs.lib.cleanSource ./.;

  # Build only the sierradb-server crate
  cargoBuildFlags = [
    "--package"
    "sierradb-server"
  ];
  cargoTestFlags = [
    "--package"
    "sierradb-server"
  ];
}
