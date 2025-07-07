{
  description = "Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      ...
    }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = function: nixpkgs.lib.genAttrs supportedSystems function;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };

          # Define the shell directly here, instead of importing
          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rustfmt"
              "rust-analyzer"
            ];
          };
          # rustToolchain = pkgs.rust-bin.selectLatestNightlyWith (
          #   toolchain:
          #   toolchain.default.override {
          #     extensions = [
          #       "rust-src"
          #       "rustfmt"
          #       "rust-analyzer"
          #     ];
          #   }
          # );

          devShell = pkgs.mkShell {
            name = "rust-dev";
            buildInputs = with pkgs; [
              bacon
              cargo-expand
              cargo-generate
              cargo-msrv
              cargo-outdated
              cargo-semver-checks
              cargo-temp
              rustToolchain
              pkg-config
              openssl.dev
            ];
          };
        in
        {
          default = devShell;
        }
      );
    };
}
