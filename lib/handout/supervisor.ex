defmodule Handoff.Supervisor do
  @moduledoc """
  Main supervisor for the Handoff execution engine.

  Manages the lifecycle of all components needed for DAG execution.
  """

  use Supervisor
  require Logger

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    # Attach telemetry handlers
    setup_telemetry_handlers()

    children = [
      {Handoff.ResultStore, []},
      {Handoff.SimpleResourceTracker, []},
      {Handoff.DataLocationRegistry, []},
      {Handoff.Executor, []},
      {Handoff.DistributedExecutor, opts},
      {Handoff.DistributedResultStore, []}
    ]

    Logger.info("Starting Handoff supervisor",
      distributed: Keyword.get(opts, :distributed, false)
    )

    Supervisor.init(children, strategy: :one_for_one)
  end

  # Set up telemetry event handlers
  defp setup_telemetry_handlers do
    # Get the telemetry events from config
    events = Application.get_env(:handoff, :telemetry, [])[:events] || []

    # Attach handlers for all events
    :telemetry.attach_many(
      "handoff-logger",
      events,
      &handle_telemetry_event/4,
      nil
    )

    # Log that we've attached the handlers
    Logger.info("Attached telemetry handlers for #{length(events)} events")
  end

  # Handle telemetry events
  defp handle_telemetry_event(event_name, measurements, metadata, _config) do
    # Log telemetry events at debug level
    event_str = event_name |> Enum.join(".")

    Logger.debug("Telemetry event: #{event_str}",
      measurements: measurements,
      metadata: metadata
    )
  end
end
