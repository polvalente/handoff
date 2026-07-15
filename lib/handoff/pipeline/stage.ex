defmodule Handoff.Pipeline.Stage do
  @moduledoc """
  GenStage `:producer_consumer` for a single DAG node in a streaming pipeline.

  Runs `:init` once on start (or once per worker when `:parallelism` > 1) to
  build persistent worker state, joins fan-in inputs by `correlation_id`, and
  invokes `:code` once per ready item.

  ## Parallelism

  When `function.parallelism` is greater than 1, this stage keeps a **single**
  GenStage process for join buffering and `BroadcastDispatcher` fan-out, and
  starts N internal `Handoff.Pipeline.Stage.Worker` processes that each run
  `:init` once. Ready `{correlation_id, args}` tuples are partitioned onto
  workers by `rem(correlation_id, N)` and processed concurrently.

  Replicas are **not** subscribed as separate GenStage consumers of upstream
  producers. Upstream stages use `BroadcastDispatcher` for DAG fan-out (every
  distinct downstream *node* must see every event); N GenStage replicas on
  that dispatcher would each receive every event and N-fire `:code`. GenStage
  demand-based load balancing among replicas only applies with
  `DemandDispatcher`, which cannot coexist with the broadcast fan-out this
  pipeline requires — so parallelism is implemented inside the stage after
  the join, not via competing subscriptions.
  """

  use GenStage

  alias Handoff.Pipeline.Stage.Worker

  @doc false
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @doc """
  Returns the pids of internal parallelism workers (empty when parallelism is 1).
  """
  def worker_pids(stage) do
    GenServer.call(stage, :worker_pids)
  end

  @impl true
  def init(opts) do
    function = Keyword.fetch!(opts, :function)
    producers = Keyword.get(opts, :producers, [])
    parallelism = max(function.parallelism || 1, 1)

    {workers, worker_state} =
      if parallelism > 1 do
        workers =
          Enum.map(1..parallelism, fn _ ->
            {:ok, pid} = Worker.start(function)
            pid
          end)

        {workers, nil}
      else
        {[], run_init(function.init)}
      end

    subscribe_to =
      Enum.map(producers, fn {pid, dep_id} ->
        {pid, max_demand: 10, min_demand: 1, dep_id: dep_id}
      end)

    state = %{
      function: function,
      worker_state: worker_state,
      workers: workers,
      join: %{},
      from_to_dep: %{}
    }

    {:producer_consumer, state, dispatcher: GenStage.BroadcastDispatcher,
     subscribe_to: subscribe_to}
  end

  @impl true
  def handle_subscribe(:producer, opts, from, state) do
    dep_id = Keyword.fetch!(opts, :dep_id)
    {:automatic, put_in(state.from_to_dep[from], dep_id)}
  end

  def handle_subscribe(:consumer, _opts, _from, state) do
    {:automatic, state}
  end

  @impl true
  def handle_call(:worker_pids, _from, state) do
    {:reply, state.workers, [], state}
  end

  @impl true
  def handle_events(events, from, state) do
    dep_id = Map.fetch!(state.from_to_dep, from)

    {emitted, state} =
      Enum.reduce(events, {[], state}, fn {cid, value}, {acc, st} ->
        {outs, st} = ingest(st, cid, dep_id, value)
        {acc ++ outs, st}
      end)

    {:noreply, emitted, state}
  end

  @impl true
  def handle_info({:worker_result, cid, result}, state) do
    {:noreply, [{cid, result}], state}
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.workers, fn pid ->
      if Process.alive?(pid) do
        GenServer.stop(pid, :shutdown, 5_000)
      end
    end)

    :ok
  end

  defp ingest(state, cid, dep_id, value) do
    partial = Map.get(state.join, cid, %{})
    partial = Map.put(partial, dep_id, value)

    if ready?(state.function.args, partial) do
      args = Enum.map(state.function.args, &Map.fetch!(partial, &1))
      state = %{state | join: Map.delete(state.join, cid)}
      dispatch(state, cid, args)
    else
      {[], %{state | join: Map.put(state.join, cid, partial)}}
    end
  end

  defp dispatch(%{workers: []} = state, cid, args) do
    {result, worker_state} = invoke(state.function, state.worker_state, args)
    {[{cid, result}], %{state | worker_state: worker_state}}
  end

  defp dispatch(%{workers: workers} = state, cid, args) do
    worker = Enum.at(workers, rem(cid, length(workers)))
    Worker.process_async(worker, self(), cid, args)
    {[], state}
  end

  defp ready?(args, partial) do
    Enum.all?(args, &Map.has_key?(partial, &1))
  end

  defp invoke(%{init: nil} = function, _worker_state, args) do
    call_args =
      case function.argument_inclusion do
        :variadic -> args ++ function.extra_args
        :as_list -> [args | function.extra_args]
      end

    {apply_code(function.code, call_args), nil}
  end

  defp invoke(function, worker_state, args) do
    call_args =
      case function.argument_inclusion do
        :variadic -> [worker_state | args] ++ function.extra_args
        :as_list -> [worker_state, args | function.extra_args]
      end

    case apply_code(function.code, call_args) do
      {result, new_state} ->
        {result, new_state}

      other ->
        raise "streaming :code with :init set must return {result, new_state}, got: #{inspect(other)}"
    end
  end

  defp run_init(nil), do: nil

  defp run_init({module, fun, args}) when is_atom(module) and is_atom(fun) and is_list(args) do
    apply(module, fun, args)
  end

  defp run_init(fun) when is_function(fun, 0), do: fun.()

  defp run_init(fun) when is_function(fun) do
    raise ":init named capture must have arity 0, got: #{inspect(fun)}"
  end

  defp apply_code(code, args) when is_function(code), do: apply(code, args)
  defp apply_code({module, fun}, args), do: apply(module, fun, args)
end
