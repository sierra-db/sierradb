{
  description = "Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    zjstatus.url = "github:dj95/zjstatus";
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      zjstatus,
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

          commonInputs = with pkgs; [
            bacon
            cargo-expand
            cargo-generate
            cargo-msrv
            cargo-outdated
            cargo-semver-checks
            cargo-temp
            redis
            pkg-config
            openssl.dev
            zjstatus.packages.${system}.default
          ];

          zjHook = ''
            export ZJSTATUS_PATH="${zjstatus.packages.${system}.default}/bin/zjstatus.wasm"
            ln -sf ${zjstatus.packages.${system}.default}/bin/zjstatus.wasm ./zjstatus.wasm
          '';
        in
        {
          default = pkgs.mkShell {
            name = "rust-dev";
            buildInputs =
              commonInputs
              ++ (with pkgs; [
                (rust-bin.stable.latest.default.override {
                  extensions = [
                    "rust-src"
                    "rustfmt"
                    "rust-analyzer"
                  ];
                })
              ]);
            shellHook = zjHook;
          };

          fuzz = pkgs.mkShell {
            name = "rust-fuzz-dev";
            buildInputs =
              commonInputs
              ++ (with pkgs; [
                (rust-bin.nightly.latest.default.override {
                  extensions = [
                    "rust-src"
                    "rustfmt"
                    "rust-analyzer"
                  ];
                })
                cargo-fuzz
              ]);
            shellHook = zjHook;
          };
        }
      );
    };
}
