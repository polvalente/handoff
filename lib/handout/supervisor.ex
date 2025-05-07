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
    children = [
      {Handoff.ResultStore, []},
      {Handoff.SimpleResourceTracker, []},
      {Handoff.DataLocationRegistry, []},
      {Handoff.Executor, []},
      {Handoff.DistributedExecutor, opts},
      {Handoff.DistributedResultStore, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
