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
      packages = forAllSystems (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
        in
        {
          default = pkgs.callPackage ./default.nix { };
        }
      );

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
          ];

          hook = ''
            ulimit -n 4096
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
            shellHook = hook;
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
            shellHook = hook;
          };
        }
      );
    };
}
