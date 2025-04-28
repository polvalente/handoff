import Config

# Configure the Elixir Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id, :node, :application, :function_id, :dag_id, :duration_ms],
  level: :info,
  colors: [
    debug: :cyan,
    info: :green,
    warning: :yellow,
    error: :red
  ]

# Limit logging in production to reduce I/O
config :logger, level: :info, truncate: :infinity

# Use Telemetry for metrics
config :handout, :telemetry,
  events: [
    [:handout, :executor, :function, :start],
    [:handout, :executor, :function, :stop],
    [:handout, :executor, :function, :exception],
    [:handout, :dag, :execution, :start],
    [:handout, :dag, :execution, :stop],
    [:handout, :dag, :execution, :exception],
    [:handout, :resource_tracker, :request],
    [:handout, :resource_tracker, :allocation],
    [:handout, :resource_tracker, :release],
    [:handout, :allocator, :allocation, :start],
    [:handout, :allocator, :allocation, :stop]
  ]

# Import environment specific config
import_config "#{config_env()}.exs"
