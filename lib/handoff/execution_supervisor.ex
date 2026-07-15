defmodule Handoff.ExecutionSupervisor do
  @moduledoc """
  DynamicSupervisor that owns temporary per-DAG `Handoff.DAGRunner` processes.
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
  Starts a temporary DAGRunner child with the given options.
  """
  def start_runner(opts) when is_list(opts) do
    DynamicSupervisor.start_child(__MODULE__, {Handoff.DAGRunner, opts})
  end
end
