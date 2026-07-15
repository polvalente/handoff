defmodule Handoff.Pipeline.Coordinator do
  @moduledoc """
  GenServer that owns a live GenStage pipeline compiled from a DAG.

  Builds one `InputStage` / `Stage` per node, wires subscriptions along
  `function.args` edges, assigns monotonic correlation ids on push, and
  tears the stage tree down on stop.

  ## Distributed placement

  On start, regular functions are allocated across cluster nodes via the
  resource tracker (`allocate_and_claim/3`) or `SimpleAllocator` fallback,
  mirroring `Handoff.DAGRunner`. Claims are held for the **pipeline lifetime**
  and released exactly once in `terminate/2` (on `stop/1`, caller death, or
  crash). The resource tracker also monitors this process as a backup release
  path when terminate is skipped (`:kill`).

  Stages (and optionally pinned inputs) are started on their assigned node.
  Cross-node GenStage subscriptions use plain producer pids.

  ## Failure policy

  * **Per-item `:code` failures** are isolated inside `Handoff.Pipeline.Stage`:
    the Aggregator is notified directly and immediate consumers get a one-hop
    suppress (no stream retries). The pipeline keeps running.
  * **Stage process EXIT / node disconnect** tears down the whole pipeline
    (coordinator stops, stages shut down, held resources released).
  * **Caller death** (the process that called `Handoff.Pipeline.start/2`) stops
    the coordinator the same way.
  """

  use GenServer

  alias Handoff.Allocator.AllocationError
  alias Handoff.DAG
  alias Handoff.Pipeline.Aggregator
  alias Handoff.Pipeline.InputStage
  alias Handoff.Pipeline.Stage
  alias Handoff.SimpleAllocator
  alias Handoff.SimpleResourceTracker

  @doc false
  def start_link({dag, opts}) do
    GenServer.start_link(__MODULE__, {dag, opts})
  end

  @impl true
  def init({dag, opts}) do
    Process.flag(:trap_exit, true)

    case DAG.validate(dag) do
      :ok ->
        start_validated_pipeline(dag, opts)

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp start_validated_pipeline(dag, opts) do
    caller_monitor = maybe_monitor_caller(Keyword.get(opts, :caller_pid))

    resource_tracker =
      Keyword.get(opts, :resource_tracker) ||
        Application.get_env(:handoff, :resource_tracker, SimpleResourceTracker)

    nodes = Keyword.get(opts, :nodes) || cluster_nodes()
    pipeline_opts = Keyword.take(opts, [:join_timeout, :max_demand, :min_demand])

    case allocate_pipeline(dag, resource_tracker, nodes) do
      {:ok, dag, held_resources} ->
        finish_pipeline_start(
          dag,
          resource_tracker,
          held_resources,
          pipeline_opts,
          caller_monitor
        )

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp finish_pipeline_start(dag, resource_tracker, held_resources, pipeline_opts, caller_monitor) do
    case build_pipeline(dag, pipeline_opts) do
      {:ok, stages, aggregator, input_ids} ->
        {:ok,
         %{
           dag: dag,
           stages: stages,
           aggregator: aggregator,
           input_ids: input_ids,
           next_cid: 0,
           resource_tracker: resource_tracker,
           held_resources: held_resources,
           caller_monitor: caller_monitor
         }}

      {:error, reason} ->
        release_held(resource_tracker, held_resources)
        {:stop, reason}
    end
  end

  defp maybe_monitor_caller(pid) when is_pid(pid), do: Process.monitor(pid)
  defp maybe_monitor_caller(_), do: nil

  @impl true
  def handle_call(:get_handle, _from, state) do
    handle = %Handoff.Pipeline{
      coordinator: self(),
      aggregator: state.aggregator,
      stages: state.stages
    }

    {:reply, handle, state}
  end

  def handle_call({:push, values}, _from, state) do
    case normalize_inputs(values, state.input_ids) do
      {:ok, input_map} ->
        cid = state.next_cid

        Enum.each(input_map, fn {input_id, value} ->
          pid = Map.fetch!(state.stages, input_id)
          InputStage.enqueue(pid, {cid, value})
        end)

        {:reply, {:ok, cid}, %{state | next_cid: cid + 1}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{caller_monitor: ref} = state) do
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, _pid, reason}, state) when reason in [:normal, :shutdown] do
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    # Remote stage death / node disconnect: tear down the whole pipeline.
    {:stop, reason, state}
  end

  @impl true
  def terminate(_reason, state) do
    stop_pids(
      Enum.filter([state.aggregator | Map.values(state.stages)], fn pid ->
        is_pid(pid) and stage_pid_alive?(pid)
      end)
    )

    release_held(state.resource_tracker, state.held_resources)
    :ok
  end

  defp allocate_pipeline(dag, tracker, nodes) do
    allocations = allocate_and_claim(dag, tracker, nodes)
    dag = assign_nodes_to_functions(dag, allocations)
    held = initial_held_resources(dag, allocations)
    {:ok, dag, held}
  rescue
    e in [AllocationError] ->
      {:error, {:allocation_error, e.message}}
  catch
    :error, reason ->
      {:error, reason}
  end

  defp allocate_and_claim(dag, tracker, nodes) do
    regular_functions =
      dag.functions
      |> Map.values()
      |> Enum.filter(fn func -> func.type == :regular end)
      |> Enum.sort_by(fn func -> func.id end)

    if function_exported?(tracker, :allocate_and_claim, 3) do
      case tracker.allocate_and_claim(regular_functions, nodes, self()) do
        {:ok, allocations} ->
          allocations

        {:error, :resources_unavailable} ->
          raise AllocationError,
                "Resources unavailable while allocating pipeline stages across #{inspect(nodes)}"

        {:error, {:allocation_error, message}} ->
          raise AllocationError, message

        {:error, reason} ->
          raise AllocationError, "Allocation failed: #{inspect(reason)}"
      end
    else
      node_caps =
        Enum.reduce(nodes, %{}, fn node, acc ->
          case :rpc.call(node, tracker, :get_capabilities, []) do
            {:badrpc, _} -> acc
            caps when is_map(caps) -> Map.put(acc, node, caps)
          end
        end)

      SimpleAllocator.allocate(regular_functions, node_caps)
    end
  end

  defp assign_nodes_to_functions(dag, allocations) do
    updated_functions =
      Enum.reduce(allocations, dag.functions, fn {function_id, node}, acc ->
        Map.update!(acc, function_id, fn function ->
          %{function | node: node}
        end)
      end)

    %{dag | functions: updated_functions}
  end

  defp initial_held_resources(dag, allocations) do
    Enum.reduce(allocations, %{}, fn {function_id, node}, acc ->
      function = Map.fetch!(dag.functions, function_id)

      if function.cost && function.cost != %{} do
        Map.put(acc, function_id, {node, function.cost})
      else
        acc
      end
    end)
  end

  defp release_held(_tracker, held) when held == %{}, do: :ok

  defp release_held(tracker, held) do
    Enum.each(held, fn {_function_id, {node, cost}} ->
      tracker.release(node, cost)
    end)

    :ok
  end

  defp build_pipeline(dag, pipeline_opts) do
    input_ids =
      dag.functions
      |> Enum.filter(fn {_id, fun} -> fun.type == :input end)
      |> Enum.map(fn {id, _} -> id end)

    case start_stages(dag, pipeline_opts) do
      {:ok, stages} ->
        start_aggregator(dag, stages, input_ids, pipeline_opts)

      {:error, _reason} = err ->
        err
    end
  end

  defp start_stages(dag, pipeline_opts) do
    order = topological_order(dag)
    all_functions = dag.functions

    Enum.reduce_while(order, {:ok, %{}}, fn id, {:ok, stages_acc} ->
      function = Map.fetch!(all_functions, id)

      case start_function_stage(function, stages_acc, all_functions, pipeline_opts) do
        :skip ->
          {:cont, {:ok, stages_acc}}

        {:ok, pid} ->
          Process.link(pid)
          {:cont, {:ok, Map.put(stages_acc, id, pid)}}

        {:error, reason} ->
          stop_pids(Map.values(stages_acc))
          {:halt, {:error, reason}}
      end
    end)
  end

  # Inline nodes are absorbed into dependents — no GenStage process.
  defp start_function_stage(%{type: :inline}, _stages, _all_functions, _opts), do: :skip

  defp start_function_stage(%{type: :input} = function, _stages, _all_functions, _opts) do
    node = function.node || Node.self()
    start_on_node(node, InputStage, id: function.id)
  end

  defp start_function_stage(function, stages, all_functions, pipeline_opts) do
    producers = producer_subscriptions(function, stages, all_functions)
    node = function.node || Node.self()

    start_on_node(
      node,
      Stage,
      [
        function: function,
        producers: producers,
        all_functions: all_functions
      ] ++ pipeline_opts
    )
  end

  defp producer_subscriptions(function, stages, all_functions) do
    function.args
    |> Enum.flat_map(&leaf_dep_ids(&1, all_functions))
    |> Enum.uniq()
    |> Enum.map(fn dep_id ->
      {Map.fetch!(stages, dep_id), dep_id}
    end)
  end

  defp leaf_dep_ids(dep_id, all_functions) do
    case Map.fetch!(all_functions, dep_id) do
      %{type: :inline} = inline ->
        Enum.flat_map(inline.args, &leaf_dep_ids(&1, all_functions))

      _other ->
        [dep_id]
    end
  end

  defp start_aggregator(dag, stages, input_ids, pipeline_opts) do
    sink_producers =
      Enum.map(sink_ids(dag), fn sink_id ->
        {Map.fetch!(stages, sink_id), sink_id}
      end)

    case Aggregator.start_link([sinks: sink_producers] ++ pipeline_opts) do
      {:ok, aggregator} ->
        Process.link(aggregator)
        wire_aggregators(dag, stages, aggregator)
        {:ok, stages, aggregator, input_ids}

      {:error, reason} ->
        stop_pids(Map.values(stages))
        {:error, reason}
    end
  end

  defp wire_aggregators(dag, stages, aggregator) do
    Enum.each(dag.functions, fn {id, fun} ->
      if fun.type not in [:input, :inline] do
        :ok = GenServer.call(Map.fetch!(stages, id), {:set_aggregator, aggregator})
      end
    end)
  end

  defp stop_pids(pids) do
    Enum.each(pids, fn pid ->
      try do
        GenStage.stop(pid, :shutdown, 5_000)
      catch
        :exit, _ -> :ok
      end
    end)
  end

  # GenStage.start (not start_link) under RPC — start_link would link to the
  # transient RPC process and the stage would die when the call returns.
  defp start_on_node(target, module, opts) do
    result =
      if target == Node.self() do
        GenStage.start(module, opts)
      else
        :rpc.call(target, GenStage, :start, [module, opts, []])
      end

    case result do
      {:ok, pid} = ok when is_pid(pid) ->
        ok

      {:error, _} = err ->
        err

      {:badrpc, reason} ->
        {:error, {:badrpc, reason}}

      other ->
        {:error, {:remote_start_failed, other}}
    end
  end

  defp stage_pid_alive?(pid) do
    if node(pid) == Node.self() do
      Process.alive?(pid)
    else
      # Process.alive?/1 is local-only; assume remote pid may still be reachable.
      true
    end
  end

  defp cluster_nodes do
    Enum.uniq([Node.self() | Node.list(:connected)])
  end

  defp sink_ids(dag) do
    referenced =
      dag.functions
      |> Map.values()
      |> Enum.flat_map(& &1.args)
      |> MapSet.new()

    dag.functions
    |> Enum.reject(fn {id, fun} ->
      fun.type == :inline or MapSet.member?(referenced, id)
    end)
    |> Enum.map(fn {id, _} -> id end)
  end

  defp topological_order(dag) do
    indegree =
      Map.new(dag.functions, fn {id, fun} -> {id, length(fun.args)} end)

    queue =
      indegree
      |> Enum.filter(fn {_id, d} -> d == 0 end)
      |> Enum.map(fn {id, _} -> id end)

    do_topo(queue, indegree, dag.functions, [])
  end

  defp do_topo([], indegree, _functions, acc) do
    if map_size(indegree) == 0 do
      Enum.reverse(acc)
    else
      # Should not happen after DAG.validate/1
      Enum.reverse(acc, Map.keys(indegree))
    end
  end

  defp do_topo([id | rest], indegree, functions, acc) do
    indegree = Map.delete(indegree, id)

    consumers =
      functions
      |> Enum.filter(fn {_cid, fun} -> id in fun.args end)
      |> Enum.map(fn {cid, _} -> cid end)

    {indegree, newly_ready} =
      Enum.reduce(consumers, {indegree, []}, fn consumer_id, {ind, ready} ->
        case Map.get(ind, consumer_id) do
          nil ->
            {ind, ready}

          d ->
            d = d - 1
            ind = Map.put(ind, consumer_id, d)
            if d == 0, do: {ind, [consumer_id | ready]}, else: {ind, ready}
        end
      end)

    do_topo(rest ++ Enum.reverse(newly_ready), indegree, functions, [id | acc])
  end

  defp normalize_inputs(values, [single_id]) when not is_map(values) do
    {:ok, %{single_id => values}}
  end

  defp normalize_inputs(values, input_ids) when is_map(values) do
    missing = Enum.reject(input_ids, &Map.has_key?(values, &1))

    if missing == [] do
      {:ok, Map.take(values, input_ids)}
    else
      {:error, {:missing_inputs, missing}}
    end
  end

  defp normalize_inputs(_values, input_ids) do
    {:error, {:expected_input_map, input_ids}}
  end
end
