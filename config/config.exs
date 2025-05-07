import Config

# Use Telemetry for metrics
config :handoff, :telemetry,
  events: [
    [:handoff, :executor, :function, :start],
    [:handoff, :executor, :function, :stop],
    [:handoff, :executor, :function, :exception],
    [:handoff, :dag, :execution, :start],
    [:handoff, :dag, :execution, :stop],
    [:handoff, :dag, :execution, :exception],
    [:handoff, :resource_tracker, :request],
    [:handoff, :resource_tracker, :allocation],
    [:handoff, :resource_tracker, :release],
    [:handoff, :allocator, :allocation, :start],
    [:handoff, :allocator, :allocation, :stop]
  ]

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
# Import environment specific config
config :logger, level: :info, truncate: :infinity

import_config "#{config_env()}.exs"
