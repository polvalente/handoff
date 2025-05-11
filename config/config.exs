import Config

if config_env() in [:dev, :test] do
  import_config "#{config_env()}.exs"
end
