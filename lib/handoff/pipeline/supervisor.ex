defmodule Handoff.Pipeline.Supervisor do
  @moduledoc """
  DynamicSupervisor that owns long-lived `Handoff.Pipeline.Coordinator` processes.
  """

  use DynamicSupervisor

  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Starts a pipeline coordinator for the given DAG.
  """
  def start_coordinator(dag, opts \\ []) do
    spec = %{
      id: make_ref(),
      start: {Handoff.Pipeline.Coordinator, :start_link, [{dag, opts}]},
      # Pipelines are caller-owned; do not restart a dead coordinator (would
      # rebuild a disconnected stage tree and can trip supervisor intensity).
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }

    DynamicSupervisor.start_child(__MODULE__, spec)
  end
end
