defmodule Handout.Supervisor do
  @moduledoc """
  Main supervisor for the Handout execution engine.

  Manages the lifecycle of all components needed for DAG execution.
  """

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Handout.ResultStore, []},
      {Handout.SimpleResourceTracker, []},
      {Handout.Executor, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
