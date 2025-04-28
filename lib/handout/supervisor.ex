defmodule Handout.Supervisor do
  @moduledoc """
  Main supervisor for the Handout execution engine.

  Manages the lifecycle of all components needed for DAG execution.
  """

  use Supervisor
  require Logger
  alias Handout.Logger, as: HandoutLogger

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    # Choose which resource tracker to use (dynamic or simple)
    resource_tracker =
      case Keyword.get(opts, :resource_tracker, :simple) do
        :dynamic -> {Handout.DynamicResourceTracker, []}
        _ -> {Handout.SimpleResourceTracker, []}
      end

    # Attach telemetry handlers
    setup_telemetry_handlers()

    children = [
      {Handout.ResultStore, []},
      resource_tracker,
      {Handout.Executor, []},
      {Handout.DistributedExecutor, opts},
      {Handout.DistributedResultStore, []}
    ]

    HandoutLogger.info("Starting Handout supervisor",
      resource_tracker: elem(resource_tracker, 0),
      distributed: Keyword.get(opts, :distributed, false)
    )

    Supervisor.init(children, strategy: :one_for_one)
  end

  # Set up telemetry event handlers
  defp setup_telemetry_handlers do
    # Get the telemetry events from config
    events = Application.get_env(:handout, :telemetry, [])[:events] || []

    # Attach handlers for all events
    :telemetry.attach_many(
      "handout-logger",
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

    HandoutLogger.debug("Telemetry event: #{event_str}",
      measurements: measurements,
      metadata: metadata
    )
  end
end
