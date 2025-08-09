{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/basics/
  env.GREET = "devenv";
  env.EDITOR = "nvim";

  hosts."127.0.0.1" = "localhost";

  # https://devenv.sh/packages/
  packages = [ pkgs.git pkgs.neovim ];

  # https://devenv.sh/languages/
  languages.go.enable = true;

  # https://devenv.sh/processes/
  # processes.cargo-watch.exec = "cargo-watch";

  # https://devenv.sh/services/
  services.temporal = {
    enable = true;
  };

  # services.clickhouse = {
  #   enable = true;
  #   config = ''
  #     cgroups_memory_usage_observer_wait_time: 0
  #   '';
  # };

  services.redis = {
    enable = true;
    # bind = "localhost";
  };

  services.postgres = {
    enable = true;
    listen_addresses = "localhost";
    package = pkgs.postgresql_17;
    # password leak!! oh well
    initialDatabases = [{ name = "mindex"; user = "immz"; pass = "dev123"; }];
    extensions = extensions: [
      extensions.timescaledb
    ];
    settings.shared_preload_libraries = "timescaledb";
    initialScript = "CREATE EXTENSION IF NOT EXISTS timescaledb; CREATE ROLE immz;";
  };

  # https://devenv.sh/scripts/
  scripts.hello.exec = ''
    echo hello from $GREET
  '';

  enterShell = ''
    hello
    git --version
  '';

  # https://devenv.sh/tasks/
  # tasks = {
  #   "myproj:setup".exec = "mytool build";
  #   "devenv:enterShell".after = [ "myproj:setup" ];
  # };

  # https://devenv.sh/tests/
  enterTest = ''
    echo "Running tests"
    git --version | grep --color=auto "${pkgs.git.version}"
  '';

  # https://devenv.sh/git-hooks/
  # git-hooks.hooks.shellcheck.enable = true;

  # See full reference at https://devenv.sh/reference/options/
}
