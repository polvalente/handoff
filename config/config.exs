import Config

# Configure the resource tracker module
config :handoff, resource_tracker: Handoff.SimpleResourceTracker

if config_env() in [:dev, :test] do
  import_config "#{config_env()}.exs"
end
