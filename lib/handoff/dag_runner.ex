defmodule Handoff.DAGRunner do
  @moduledoc """
  Per-DAG event-driven scheduler.

  Owns resource claims for the run, launches bounded workers via
  `Handoff.TaskSupervisor`, and reports a single terminal outcome to
  `Handoff.DistributedExecutor`.
  """

  use GenServer

  alias Handoff.Allocator.AllocationError
  alias Handoff.DataLocationRegistry
  alias Handoff.FunctionRunner
  alias Handoff.ResultStore

  require Logger

  def child_spec(opts) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    dag = Keyword.fetch!(opts, :dag)
    executor = Keyword.fetch!(opts, :executor)
    resource_tracker = Keyword.fetch!(opts, :resource_tracker)
    nodes = Keyword.fetch!(opts, :nodes)
    caller_pid = Keyword.fetch!(opts, :caller_pid)
    max_retries = Keyword.get(opts, :max_retries, 3)
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    caller_monitor = Process.monitor(caller_pid)

    state = %{
      dag: dag,
      executor: executor,
      resource_tracker: resource_tracker,
      nodes: nodes,
      caller_pid: caller_pid,
      caller_monitor: caller_monitor,
      max_retries: max_retries,
      max_concurrency: max(1, max_concurrency),
      allocations: %{},
      held_resources: %{},
      to_be_executed: MapSet.new(),
      ready_queue: :queue.new(),
      in_flight: %{},
      executed: %{},
      status: :starting
    }

    {:ok, state, {:continue, :start_execution}}
  end

  @impl true
  def handle_continue(:start_execution, state) do
    allocations = allocate_and_claim(state)
    dag = assign_nodes_to_functions(state.dag, allocations)
    held_resources = initial_held_resources(dag, allocations)

    to_be_executed =
      dag
      |> topological_ids()
      |> Enum.reduce(MapSet.new(), fn function_id, pending ->
        function = Map.fetch!(dag.functions, function_id)

        if function.type == :inline do
          pending
        else
          MapSet.put(pending, function_id)
        end
      end)

    state = %{
      state
      | dag: dag,
        allocations: allocations,
        held_resources: held_resources,
        to_be_executed: to_be_executed,
        status: :running
    }

    state
    |> enqueue_ready()
    |> launch_ready()
    |> reply_or_continue()
  rescue
    e in [AllocationError] ->
      Logger.error("Allocation error during DAG execution: #{e.message}")
      finish(state, {:error, {:allocation_error, e.message}})

    exception ->
      finish(state, {:error, {:exception, Exception.message(exception)}})
  end

  @impl true
  def handle_info({ref, outcome}, %{in_flight: in_flight} = state)
      when is_map_key(in_flight, ref) do
    Process.demonitor(ref, [:flush])
    {%{function_id: function_id}, in_flight} = Map.pop!(in_flight, ref)
    state = %{state | in_flight: in_flight}

    case outcome do
      {:ok, result} ->
        state
        |> record_success(function_id, result)
        |> enqueue_ready()
        |> launch_ready()
        |> reply_or_continue()

      {:error, reason} ->
        fail_and_cancel(state, {:error, {:function_failed, function_id, reason}})
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{in_flight: in_flight} = state)
      when is_map_key(in_flight, ref) do
    {%{function_id: function_id}, in_flight} = Map.pop!(in_flight, ref)
    state = %{state | in_flight: in_flight}

    fail_and_cancel(
      state,
      {:error, {:function_failed, function_id, {:worker_crashed, reason}}}
    )
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{caller_monitor: ref} = state) do
    state = cancel_in_flight(state)
    state = release_all_held(state)
    send(state.executor, {:dag_finished, self(), {:error, :caller_gone}})
    {:stop, :normal, %{state | status: :done, caller_monitor: nil}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp reply_or_continue(%{sync_failure: outcome} = state) do
    fail_and_cancel(Map.delete(state, :sync_failure), outcome)
  end

  defp reply_or_continue(state) do
    if dag_complete?(state) do
      finish(
        state,
        {:ok,
         %{
           dag_id: state.dag.id,
           results: state.executed,
           allocations: state.allocations
         }}
      )
    else
      {:noreply, state}
    end
  end

  defp dag_complete?(state) do
    state.status == :running and MapSet.size(state.to_be_executed) == 0 and
      map_size(state.in_flight) == 0 and :queue.is_empty(state.ready_queue)
  end

  defp finish(state, outcome) do
    state = release_all_held(state)
    send(state.executor, {:dag_finished, self(), outcome})
    {:stop, :normal, %{state | status: :done}}
  end

  defp fail_and_cancel(state, outcome) do
    state
    |> Map.put(:status, :failing)
    |> cancel_in_flight()
    |> finish(outcome)
  end

  defp cancel_in_flight(state) do
    refs = Map.keys(state.in_flight)

    Enum.each(state.in_flight, fn {ref, %{pid: pid}} ->
      Process.demonitor(ref, [:flush])
      Process.exit(pid, :kill)
    end)

    # Drain only known task result tuples; DOWN messages were flushed via demonitor.
    # Do not receive arbitrary :DOWN here — that would swallow the caller monitor.
    Enum.each(refs, fn ref ->
      receive do
        {^ref, _outcome} -> :ok
      after
        0 -> :ok
      end
    end)

    %{state | in_flight: %{}}
  end

  defp record_success(state, function_id, result) do
    function = Map.fetch!(state.dag.functions, function_id)

    stored_result =
      case result do
        {:remote_store_and_registry_ok, _fun_id, _node} ->
          :remote_executed_and_registered

        other ->
          :ok = ResultStore.store(state.dag.id, function_id, other)
          DataLocationRegistry.register(state.dag.id, function_id, function.node || Node.self())
          other
      end

    state
    |> release_function_resources(function_id)
    |> Map.update!(:executed, &Map.put(&1, function_id, stored_result))
  end

  defp enqueue_ready(state) do
    for function_id <- state.to_be_executed,
        function = Map.fetch!(state.dag.functions, function_id),
        all_deps_satisfied?(function, state.dag, state.executed),
        reduce: state do
      acc ->
        %{
          acc
          | to_be_executed: MapSet.delete(acc.to_be_executed, function_id),
            ready_queue: :queue.in(function_id, acc.ready_queue)
        }
    end
  end

  defp launch_ready(%{status: status} = state) when status != :running, do: state

  defp launch_ready(state) do
    if map_size(state.in_flight) >= state.max_concurrency or :queue.is_empty(state.ready_queue) do
      state
    else
      {{:value, function_id}, ready_queue} = :queue.out(state.ready_queue)
      state = %{state | ready_queue: ready_queue}

      case launch_function(state, function_id) do
        {:ok, state} ->
          launch_ready(state)

        {:error, state, outcome} ->
          Map.put(state, :sync_failure, outcome)
      end
    end
  end

  defp launch_function(state, function_id) do
    function = Map.fetch!(state.dag.functions, function_id)

    try do
      args =
        FunctionRunner.fetch_arguments(
          state.dag.id,
          function.args,
          state.executed,
          function.node,
          state.dag.functions
        )

      max_retries = resolve_max_retries(function, state.max_retries)

      task =
        Task.Supervisor.async_nolink(Handoff.TaskSupervisor, fn ->
          FunctionRunner.execute(
            state.dag.id,
            function,
            args,
            max_retries,
            state.dag.functions
          )
        end)

      in_flight =
        Map.put(state.in_flight, task.ref, %{
          function_id: function_id,
          pid: task.pid,
          task: task
        })

      {:ok, %{state | in_flight: in_flight}}
    rescue
      e in [AllocationError] ->
        {:error, state, {:error, {:allocation_error, e.message}}}

      e ->
        {:error, state, {:error, {:function_failed, function_id, Exception.message(e)}}}
    end
  end

  defp resolve_max_retries(%{max_retries: nil}, default), do: default
  defp resolve_max_retries(%{max_retries: max_retries}, _default), do: max_retries

  defp all_deps_satisfied?(function, dag, executed) do
    Enum.all?(function.args, fn arg_id ->
      dep_function_def = Map.get(dag.functions, arg_id)

      cond do
        is_nil(dep_function_def) ->
          true

        dep_function_def.type == :inline ->
          true

        Map.has_key?(executed, arg_id) ->
          true

        true ->
          false
      end
    end)
  end

  defp allocate_and_claim(state) do
    tracker = state.resource_tracker
    nodes = state.nodes
    dag = state.dag

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
                "Resources unavailable while allocating functions across #{inspect(nodes)}"

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

      Handoff.SimpleAllocator.allocate(regular_functions, node_caps)
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

  defp release_function_resources(state, function_id) do
    case Map.pop(state.held_resources, function_id) do
      {nil, held} ->
        %{state | held_resources: held}

      {{node, cost}, held} ->
        state.resource_tracker.release(node, cost)
        %{state | held_resources: held}
    end
  end

  defp release_all_held(state) do
    Enum.each(state.held_resources, fn {_function_id, {node, cost}} ->
      state.resource_tracker.release(node, cost)
    end)

    %{state | held_resources: %{}}
  end

  defp topological_ids(dag) do
    {sorted, _} =
      Enum.reduce(Map.keys(dag.functions), {[], MapSet.new()}, fn id, {sorted, visited} ->
        visit(dag, id, sorted, visited)
      end)

    Enum.reverse(sorted)
  end

  defp visit(dag, id, sorted, visited) do
    if MapSet.member?(visited, id) do
      {sorted, visited}
    else
      visited = MapSet.put(visited, id)
      function = Map.get(dag.functions, id)

      {sorted, visited} =
        Enum.reduce(function.args, {sorted, visited}, fn dep_id, {sorted_acc, visited_acc} ->
          visit(dag, dep_id, sorted_acc, visited_acc)
        end)

      {[id | sorted], visited}
    end
  end
end
