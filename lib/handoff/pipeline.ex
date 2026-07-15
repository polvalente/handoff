defmodule Handoff.Pipeline do
  @moduledoc """
  Public API for long-lived GenStage streaming pipelines.

  A pipeline is compiled once from a DAG via `start/2` (or `Handoff.stream/2`).
  Items are pushed with `push/2` and results consumed via `stream/1` in push order.
  """

  @type t :: %__MODULE__{
          coordinator: pid(),
          aggregator: pid(),
          stages: %{optional(term()) => pid()}
        }

  @enforce_keys [:coordinator, :aggregator, :stages]
  defstruct [:coordinator, :aggregator, :stages]

  @doc """
  Starts a streaming pipeline for `dag`.

  Returns `{:ok, handle}` without waiting for any items to be processed.
  """
  def start(dag, opts \\ []) do
    case Handoff.Pipeline.Supervisor.start_coordinator(dag, opts) do
      {:ok, pid} ->
        handle = GenServer.call(pid, :get_handle)
        {:ok, handle}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Pushes an item into the pipeline.

  With a single `:input` node, `values` may be a bare term. With multiple
  inputs, pass a map of `%{input_id => value}`.

  Returns `{:ok, correlation_id}` assigned for this push.
  """
  def push(%__MODULE__{coordinator: pid}, values) do
    GenServer.call(pid, {:push, values})
  end

  @doc """
  Returns an enumerable of pipeline outputs in push order.

  Built on `GenStage.stream/1` over the pipeline aggregator.
  """
  def stream(%__MODULE__{aggregator: aggregator}) do
    GenStage.stream([{aggregator, max_demand: 10}])
  end

  @doc """
  Stops the pipeline and tears down all stage processes.
  """
  def stop(%__MODULE__{coordinator: pid}) do
    GenServer.stop(pid, :normal)
  end
end
