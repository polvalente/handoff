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
    # Read resource_tracker from application config, with fallback to opts for backward compatibility
    resource_tracker =
      Application.get_env(:handoff, :resource_tracker) ||
        Keyword.get(opts, :resource_tracker, Handoff.SimpleResourceTracker)

    children = [
      {Handoff.ResultStore, []},
      resource_tracker,
      {Handoff.DataLocationRegistry, []},
      {Handoff.DistributedExecutor, Keyword.put(opts, :resource_tracker, resource_tracker)},
      {Handoff.DistributedResultStore, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
