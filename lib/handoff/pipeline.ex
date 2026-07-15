defmodule Handoff.Pipeline do
  @moduledoc """
  Public API for long-lived GenStage streaming pipelines.

  A pipeline is compiled once from a DAG via `start/2` (or `Handoff.stream/2`).
  Items are pushed with `push/2` and results consumed via `stream/1` in push order.

  ## Options

  Passed through to the coordinator / stages:

  * `:join_timeout` — milliseconds before a partial fan-in join is evicted as
    `{:error, :join_timeout}` (Stage and Aggregator). Default: no eviction.
  * `:max_demand` / `:min_demand` — GenStage subscription demand (default `10` / `1`).
  * `:nodes` / `:resource_tracker` — distributed placement (see Coordinator).

  ## Failure semantics

  Per-item `:code` exceptions notify the Aggregator directly as
  `{:error, reason}` and suppress further propagation past immediate consumers;
  the pipeline keeps running. There is **no** stream-mode retry analogous to
  `Handoff.execute/2`'s `:max_retries`. Stage process death or node loss still
  tears down the whole pipeline.
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
  Monitors the calling process; if it exits, the pipeline stops and releases
  resource claims.
  """
  def start(dag, opts \\ []) do
    opts = Keyword.put_new(opts, :caller_pid, self())

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

  Idempotent: returns `:ok` if the coordinator is already dead. Resource claims
  are released exactly once (in coordinator `terminate/2`, with tracker
  monitoring as a backup).
  """
  def stop(%__MODULE__{coordinator: pid}) do
    if Process.alive?(pid) do
      GenServer.stop(pid, :normal)
    else
      :ok
    end
  end
end
