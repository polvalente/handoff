defmodule Handoff.Pipeline.Stage do
  @moduledoc """
  GenStage `:producer_consumer` for a single DAG node in a streaming pipeline.

  Runs `:init` once on start (or once per worker when `:parallelism` > 1) to
  build persistent worker state, joins fan-in inputs by `correlation_id`, and
  invokes `:code` once per ready item (or once per batch when batching is on).

  Dependencies of type `:inline` are not upstream GenStage producers: this stage
  subscribes to their transitive non-inline leaves and evaluates the inline
  `:code` when resolving args (same absorb-into-dependent model as batch execute).

  ## Failure semantics

  Exceptions raised by `:code` (or contract violations) are **rescued per item**.
  The originating stage notifies the pipeline `Aggregator` directly with
  `{:item_error, cid, reason}` and emits a one-hop `{cid, {:suppress, reason}}`
  to immediate GenStage consumers so they drop that correlation id without
  invoking `:code` or forwarding further. Sibling branches keep running.
  Stream mode does **not** retry (`Function.max_retries` is execute-only).

  A hard EXIT of this GenStage process (or its host node) still tears down the
  whole pipeline — see `Handoff.Pipeline.Coordinator`.

  ## Backpressure

  Upstream subscriptions use GenStage defaults of `max_demand: 10` and
  `min_demand: 1` (overridable via `:max_demand` / `:min_demand` on
  `Handoff.stream/2`).

  ## Join eviction

  When `:join_timeout` (ms) is set on the pipeline, partial fan-in buffers for a
  correlation id are dropped after the timeout via the same fail path (direct
  Aggregator notify + one-hop suppress).

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

  ## Batching

  When `function.batch_size` and/or `function.batch_timeout` is set, ready
  items are accumulated on the code-execution path (this process when
  `parallelism` is 1, each `Worker` when `parallelism` > 1) and flushed when
  the buffer reaches `batch_size` or `batch_timeout` elapses since the first
  buffered item — whichever comes first. Batched `:code` receives a list of
  arg-tuples and must return a same-length list of results, which are
  unbatched 1:1 back into individually tagged events. See `Handoff.Function`
  for the calling convention.
  """

  use GenStage

  alias Handoff.Pipeline.Invoke
  alias Handoff.Pipeline.Stage.Batch
  alias Handoff.Pipeline.Stage.Worker

  require Invoke

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
    all_functions = Keyword.get(opts, :all_functions, %{function.id => function})
    parallelism = max(function.parallelism || 1, 1)
    join_timeout = Keyword.get(opts, :join_timeout)
    max_demand = Keyword.get(opts, :max_demand, 10)
    min_demand = Keyword.get(opts, :min_demand, 1)

    {workers, worker_monitors, worker_state} =
      if parallelism > 1 do
        workers_with_refs =
          Enum.map(1..parallelism, fn _ ->
            {:ok, pid} = Worker.start(function)
            {pid, Process.monitor(pid)}
          end)

        workers = Enum.map(workers_with_refs, fn {pid, _} -> pid end)
        monitors = Map.new(workers_with_refs, fn {pid, ref} -> {ref, pid} end)
        {workers, monitors, nil}
      else
        {[], %{}, run_init(function.init)}
      end

    subscribe_to =
      Enum.map(producers, fn {pid, dep_id} ->
        {pid, max_demand: max_demand, min_demand: min_demand, dep_id: dep_id}
      end)

    leaf_deps =
      function.args
      |> Enum.flat_map(&leaf_dep_ids(&1, all_functions))
      |> Enum.uniq()

    state = %{
      function: function,
      all_functions: all_functions,
      leaf_deps: leaf_deps,
      worker_state: worker_state,
      workers: workers,
      worker_monitors: worker_monitors,
      aggregator: Keyword.get(opts, :aggregator),
      join: %{},
      join_timers: %{},
      join_timeout: join_timeout,
      from_to_dep: %{},
      batch_buffer: [],
      batch_timer: nil
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
  def handle_call({:set_aggregator, aggregator}, _from, state) when is_pid(aggregator) do
    {:reply, :ok, [], %{state | aggregator: aggregator}}
  end

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
  def handle_info({:worker_result, cid, {:error, reason}}, state) do
    {emitted, state} = fail_item(state, cid, reason)
    {:noreply, emitted, state}
  end

  def handle_info({:worker_result, cid, result}, state) do
    {:noreply, [{cid, result}], state}
  end

  def handle_info({:worker_results, pairs}, state) when is_list(pairs) do
    {emitted, state} =
      Enum.reduce(pairs, {[], state}, fn
        {cid, {:error, reason}}, {acc, st} ->
          {outs, st} = fail_item(st, cid, reason)
          {acc ++ outs, st}

        {cid, result}, {acc, st} ->
          {acc ++ [{cid, result}], st}
      end)

    {:noreply, emitted, state}
  end

  def handle_info({:batch_timeout, ref}, %{batch_timer: ref} = state) do
    {emitted, state} = flush_batch(%{state | batch_timer: nil})
    {:noreply, emitted, state}
  end

  def handle_info({:batch_timeout, _stale}, state) do
    {:noreply, [], state}
  end

  def handle_info({:join_timeout, cid}, state) do
    case Map.get(state.join, cid) do
      nil ->
        {:noreply, [], state}

      :suppressed ->
        {:noreply, [], state}

      :timed_out ->
        {:noreply, [], state}

      _partial ->
        {emitted, state} = fail_item(state, cid, :join_timeout)
        {:noreply, emitted, state}
    end
  end

  def handle_info({:gc_join, cid}, state) do
    state =
      case Map.get(state.join, cid) do
        tombstone when tombstone in [:timed_out, :suppressed] ->
          %{state | join: Map.delete(state.join, cid)}

        _ ->
          state
      end

    {:noreply, [], state}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.pop(state.worker_monitors, ref) do
      {nil, _} ->
        {:noreply, [], state}

      {^pid, monitors} ->
        # Unexpected worker death: replace the slot. In-flight cid (if any) is
        # lost unless the worker already sent an error — fire-and-forget.
        index = Enum.find_index(state.workers, &(&1 == pid))
        {:ok, new_pid} = Worker.start(state.function)
        new_ref = Process.monitor(new_pid)

        workers =
          if index do
            List.replace_at(state.workers, index, new_pid)
          else
            state.workers ++ [new_pid]
          end

        {:noreply, [],
         %{state | workers: workers, worker_monitors: Map.put(monitors, new_ref, new_pid)}}
    end
  end

  @impl true
  def terminate(_reason, state) do
    cancel_batch_timer(state)
    Enum.each(Map.keys(state.join_timers), &cancel_join_timer_ref(state, &1))

    Enum.each(state.workers, fn pid ->
      if Process.alive?(pid) do
        GenServer.stop(pid, :shutdown, 5_000)
      end
    end)

    :ok
  end

  defp ingest(state, cid, _dep_id, {:suppress, _reason}) do
    accept_suppress(state, cid)
  end

  defp ingest(state, cid, dep_id, value) do
    case Map.get(state.join, cid) do
      tombstone when tombstone in [:timed_out, :suppressed] ->
        {[], state}

      partial ->
        partial = partial || %{}
        first? = partial == %{}
        partial = Map.put(partial, dep_id, value)

        state =
          if first? do
            maybe_arm_join_timer(state, cid)
          else
            state
          end

        if ready?(state.leaf_deps, partial) do
          state = clear_join(state, cid)

          args =
            Enum.map(state.function.args, &resolve_arg(&1, partial, state.all_functions))

          dispatch(state, cid, args)
        else
          {[], %{state | join: Map.put(state.join, cid, partial)}}
        end
    end
  end

  defp accept_suppress(state, cid) do
    case Map.get(state.join, cid) do
      :suppressed ->
        {[], state}

      :timed_out ->
        {[], state}

      _ ->
        state =
          state
          |> clear_join(cid)
          |> put_suppress_tombstone(cid)

        # One-hop only: do not forward suppress further.
        {[], state}
    end
  end

  defp fail_item(state, cid, reason) do
    notify_aggregator(state, cid, reason)

    state =
      state
      |> clear_join(cid)
      |> put_suppress_tombstone(cid)

    {[{cid, {:suppress, reason}}], state}
  end

  defp notify_aggregator(%{aggregator: pid}, cid, reason) when is_pid(pid) do
    send(pid, {:item_error, cid, reason})
  end

  defp notify_aggregator(_state, _cid, _reason), do: :ok

  defp dispatch(%{workers: []} = state, cid, args) do
    if Batch.enabled?(state.function) do
      enqueue_batch(state, cid, args)
    else
      case Invoke.safe(do: invoke(state.function, state.worker_state, args)) do
        {:ok, {result, worker_state}} ->
          {[{cid, result}], %{state | worker_state: worker_state}}

        {:error, reason} ->
          fail_item(state, cid, reason)
      end
    end
  end

  defp dispatch(%{workers: workers} = state, cid, args) do
    worker = Enum.at(workers, rem(cid, length(workers)))
    Worker.process_async(worker, self(), cid, args)
    {[], state}
  end

  defp enqueue_batch(state, cid, args) do
    buffer = state.batch_buffer ++ [{cid, args}]
    state = maybe_start_batch_timer(%{state | batch_buffer: buffer})

    if Batch.full?(state.function, state.batch_buffer) do
      flush_batch(state)
    else
      {[], state}
    end
  end

  defp maybe_start_batch_timer(%{batch_timer: nil, function: %{batch_timeout: timeout}} = state)
       when is_integer(timeout) do
    ref = make_ref()
    Process.send_after(self(), {:batch_timeout, ref}, timeout)
    %{state | batch_timer: ref}
  end

  defp maybe_start_batch_timer(state), do: state

  defp flush_batch(%{batch_buffer: []} = state), do: {[], state}

  defp flush_batch(state) do
    state = cancel_batch_timer(state)
    items = state.batch_buffer
    state = %{state | batch_buffer: []}

    case Invoke.safe(do: Batch.invoke_and_unbatch(state.function, state.worker_state, items)) do
      {:ok, {pairs, worker_state}} ->
        {pairs, %{state | worker_state: worker_state}}

      {:error, reason} ->
        Enum.reduce(items, {[], state}, fn {cid, _}, {acc, st} ->
          {outs, st} = fail_item(st, cid, reason)
          {acc ++ outs, st}
        end)
    end
  end

  defp cancel_batch_timer(%{batch_timer: nil} = state), do: state

  defp cancel_batch_timer(%{batch_timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | batch_timer: nil}
  end

  defp maybe_arm_join_timer(%{join_timeout: timeout} = state, cid) when is_integer(timeout) do
    ref = Process.send_after(self(), {:join_timeout, cid}, timeout)
    %{state | join_timers: Map.put(state.join_timers, cid, ref)}
  end

  defp maybe_arm_join_timer(state, _cid), do: state

  defp put_suppress_tombstone(%{join_timeout: timeout} = state, cid) when is_integer(timeout) do
    Process.send_after(self(), {:gc_join, cid}, timeout)
    %{state | join: Map.put(state.join, cid, :suppressed)}
  end

  defp put_suppress_tombstone(state, cid) do
    # Default GC so suppress tombstones do not grow without bound.
    Process.send_after(self(), {:gc_join, cid}, 60_000)
    %{state | join: Map.put(state.join, cid, :suppressed)}
  end

  defp clear_join(state, cid) do
    state
    |> cancel_join_timer(cid)
    |> then(fn st -> %{st | join: Map.delete(st.join, cid)} end)
  end

  defp cancel_join_timer(state, cid) do
    case Map.pop(state.join_timers, cid) do
      {nil, _} ->
        state

      {ref, timers} ->
        Process.cancel_timer(ref)
        %{state | join_timers: timers}
    end
  end

  defp cancel_join_timer_ref(state, cid), do: cancel_join_timer(state, cid)

  defp ready?(leaf_deps, partial) do
    Enum.all?(leaf_deps, &Map.has_key?(partial, &1))
  end

  defp leaf_dep_ids(dep_id, all_functions) do
    case Map.fetch!(all_functions, dep_id) do
      %{type: :inline} = inline ->
        Enum.flat_map(inline.args, &leaf_dep_ids(&1, all_functions))

      _other ->
        [dep_id]
    end
  end

  defp resolve_arg(dep_id, partial, all_functions) do
    case Map.fetch!(all_functions, dep_id) do
      %{type: :inline} = inline ->
        inline_args = Enum.map(inline.args, &resolve_arg(&1, partial, all_functions))
        apply_code(inline.code, inline_args ++ inline.extra_args)

      _other ->
        Map.fetch!(partial, dep_id)
    end
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
