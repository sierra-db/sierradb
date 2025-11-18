{
  pkgs ? import <nixpkgs> { },
}:
let
  # Use nightly Rust from rust-overlay
  rustToolchain = pkgs.rust-bin.stable.latest.default;

  # Create custom rustPlatform with stable toolchain
  rustPlatform = pkgs.makeRustPlatform {
    cargo = rustToolchain;
    rustc = rustToolchain;
  };
in
rustPlatform.buildRustPackage {
  pname = "sierradb";
  version = "0.1";
  cargoLock.lockFile = ./Cargo.lock;
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
