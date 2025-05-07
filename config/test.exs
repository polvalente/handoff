import Config

# Configure ExCoveralls for test coverage
config :excoveralls,
  json_report: true,
  timeout: 60_000

config :logger, level: :info
