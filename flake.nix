{
  description = "Development environment for pg_wal_visualizer";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            bc
            just
            openssl
            pkg-config
            postgresql_16
          ];

          shellHook = ''
            export PGDATA="$PWD/.local/postgres"
            export PGHOST="$PWD/.local/postgres"
            export PGPORT="5433"
            export PGUSER="postgres"
            export PGDATABASE="pg_wal_visualizer"
            export DATABASE_URL="postgresql://$PGUSER@127.0.0.1:$PGPORT/$PGDATABASE"

            mkdir -p "$PWD/.local"

            echo "pg_wal_visualizer dev shell"
            echo "  PGDATA=$PGDATA"
            echo "  DATABASE_URL=$DATABASE_URL"
          '';
        };
      });
}
