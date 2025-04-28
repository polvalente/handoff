import Config

# Configure the Elixir Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id, :node]

# Use Telemetry for metrics
config :telemetry,
  enabled: true

# Import environment specific config
import_config "#{config_env()}.exs"
