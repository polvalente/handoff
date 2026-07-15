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
    DynamicSupervisor.start_child(__MODULE__, {Handoff.Pipeline.Coordinator, {dag, opts}})
  end
end
