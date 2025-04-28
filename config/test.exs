import Config

config :logger, level: :warning

# Configure ExCoveralls for test coverage
config :excoveralls,
  json_report: true,
  timeout: 60_000
