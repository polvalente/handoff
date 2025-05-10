defmodule Handoff.DistributedExecutor do
  @moduledoc """
  Handles the distributed execution of functions across multiple nodes.

  Provides node discovery, coordination, and remote task execution capabilities.
  """

  use GenServer

  alias Handoff.Allocator.AllocationError
  alias Handoff.DAG
  alias Handoff.DataLocationRegistry
  alias Handoff.ResultStore
  alias Handoff.SimpleResourceTracker

  require Logger

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Discovers and registers nodes in the cluster with their capabilities.

  Makes remote calls to discover nodes and their resources.
  """
  def discover_nodes do
    GenServer.call(__MODULE__, :discover_nodes)
  end

  @doc """
  Registers the local node with its capabilities and makes it available for function execution.

  ## Parameters
  - caps: Map of capabilities provided by this node (e.g., %{cpu: 8, memory: 4000})
  """
  def register_local_node(caps) do
    GenServer.call(__MODULE__, {:register_local_node, caps})
  end

  @doc """
  Executes the DAG across the distributed nodes.

  ## Parameters
  - dag: A validated DAG to execute
  - opts: Optional execution options

  ## Returns
  - {:ok, results} with a map of function IDs to results on success
  - {:error, reason} on failure
  """
  def execute(dag, opts \\ []) do
    case DAG.validate(dag) do
      :ok -> GenServer.call(__MODULE__, {:execute, dag, opts}, :infinity)
      error -> error
    end
  end

  # Server callbacks

  @impl true
  def init(opts) do
    # Get connection settings from options
    heartbeat_interval = Keyword.get(opts, :heartbeat_interval, 5000)

    # Schedule periodic heartbeat
    :timer.send_interval(heartbeat_interval, :check_nodes)

    {:ok,
     %{
       # Available nodes and their capabilities
       nodes: %{},
       # Functions currently being executed
       executing: %{},
       # PIDs being monitored with their function IDs
       monitored: %{},
       # Retry counts for failed functions
       retry_count: %{},
       max_retries: Keyword.get(opts, :max_retries, 3)
     }}
  end

  @impl true
  def handle_call(:discover_nodes, _from, state) do
    # Get all connected nodes and query their capabilities
    discovered =
      Enum.reduce([Node.self() | Node.list()], %{}, fn node, acc ->
        case :rpc.call(node, Handoff.SimpleResourceTracker, :get_capabilities, []) do
          {:badrpc, reason} ->
            Logger.warning(
              "Failed to discover capabilities for node #{inspect(node)}: #{inspect(reason)}"
            )

            acc

          capabilities when is_map(capabilities) ->
            Logger.info(
              "Discovered node #{inspect(node)} with capabilities: #{inspect(capabilities)}"
            )

            # Register the node with the resource tracker
            :ok = SimpleResourceTracker.register(node, capabilities)
            Map.put(acc, node, capabilities)
        end
      end)

    {:reply, {:ok, discovered}, %{state | nodes: Map.merge(state.nodes, discovered)}}
  end

  @impl true
  def handle_call({:register_local_node, caps}, _from, state) do
    # Register local node with the resource tracker
    :ok = SimpleResourceTracker.register(Node.self(), caps)

    # Update state with local node capabilities
    nodes = Map.put(state.nodes, Node.self(), caps)

    {:reply, :ok, %{state | nodes: nodes}}
  end

  @impl true
  def handle_call({:execute, dag, opts}, from, state) do
    # Clear any previous results for this DAG
    ResultStore.clear(dag.id)

    # Clear data location registry for this DAG
    DataLocationRegistry.clear(dag.id)

    # Register initial arguments in the data location registry for this DAG
    dag.functions
    |> Map.values()
    |> get_in([Access.all(), Access.key!(:args)])
    |> List.flatten()
    |> Enum.each(fn
      nil ->
        :ok

      arg_id ->
        # Only register if it's a literal value, not a function ID
        if not Map.has_key?(dag.functions, arg_id) do
          DataLocationRegistry.register(dag.id, arg_id, Node.self())
        end
    end)

    # If no nodes are known, discover them
    state =
      if map_size(state.nodes) == 0 do
        case handle_call(:discover_nodes, from, state) do
          {:reply, _, new_state} -> new_state
          _ -> state
        end
      else
        state
      end

    # Begin execution process
    task =
      Task.async(fn ->
        try do
          execute_dag(dag, opts, from, state.max_retries)
        rescue
          e in [AllocationError] ->
            GenServer.reply(from, {:error, {:allocation_error, e.message}})

          exception ->
            GenServer.reply(from, {:error, exception})
        end
      end)

    {:noreply, %{state | executing: Map.put(state.executing, task.ref, from)}}
  end

  @impl true
  def handle_info({ref, _result}, %{executing: executing} = state)
      when is_map_key(executing, ref) do
    {:noreply, %{state | executing: Map.delete(executing, ref)}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    # Handle process termination
    case Map.get(state.monitored, pid) do
      nil ->
        # Unknown process, ignore
        {:noreply, state}

      function_id ->
        # A monitored function execution crashed
        Logger.warning("Function #{inspect(function_id)} execution failed: #{inspect(reason)}")

        # Update monitored processes
        monitored = Map.delete(state.monitored, pid)
        executing = Map.delete(state.executing, function_id)

        # Retry mechanism would go here
        retries = Map.get(state.retry_count, function_id, 0)
        retry_count = Map.put(state.retry_count, function_id, retries + 1)

        {:noreply,
         %{state | monitored: monitored, executing: executing, retry_count: retry_count}}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, _, _, reason}, %{executing: executing} = state)
      when is_map_key(executing, ref) do
    if reason != :normal do
      # A monitored function execution crashed
      Logger.warning("Function execution crashed: #{inspect(reason)}")

      # Retry mechanism would go here
      GenServer.reply(executing[ref], {:error, reason})
    end

    {:noreply, %{state | executing: Map.delete(executing, ref)}}
  end

  @impl true
  def handle_info(:check_nodes, state) do
    # Check health of all nodes
    node_status =
      Enum.map(state.nodes, fn {node, _caps} ->
        is_alive = node == Node.self() or Node.ping(node) == :pong
        {node, is_alive}
      end)

    # Keep only alive nodes
    alive_nodes =
      Enum.reduce(node_status, state.nodes, fn
        {_node, true}, acc ->
          acc

        {node, false}, acc ->
          Logger.warning("Node #{inspect(node)} is down, removing from available nodes")
          Map.delete(acc, node)
      end)

    {:noreply, %{state | nodes: alive_nodes}}
  end

  # Private functions

  defp execute_dag(dag, opts, caller, max_retries) do
    # Allocate functions to nodes
    allocation_strategy = Keyword.get(opts, :allocation_strategy, :first_available)

    # Get node capabilities from the tracker
    node_caps =
      Enum.reduce([Node.self() | Node.list()], %{}, fn node, acc ->
        case :rpc.call(node, Handoff.SimpleResourceTracker, :get_capabilities, []) do
          {:badrpc, _} -> acc
          caps when is_map(caps) -> Map.put(acc, node, caps)
        end
      end)

    # Allocate functions to nodes
    allocations = allocate_functions(dag, node_caps, allocation_strategy)

    # Update dag functions with node assignments
    dag = assign_nodes_to_functions(dag, allocations)

    # Execute functions in topological order
    topo_sorted = topological_sort(dag)

    # Track function dependencies and completion
    to_be_executed = MapSet.new(topo_sorted)
    executed = %{}
    pending = MapSet.new()

    try do
      results =
        execute_functions_with_deps(
          dag,
          to_be_executed,
          executed,
          pending,
          max_retries
        )

      # Check if any function failed due to resource constraints
      resource_errors =
        Enum.filter(results, fn {_id, result} ->
          case result do
            {:error, %RuntimeError{message: message}} ->
              String.contains?(message, "Resources unavailable")

            _ ->
              false
          end
        end)

      if Enum.empty?(resource_errors) do
        GenServer.reply(
          caller,
          {:ok, %{dag_id: dag.id, results: results, allocations: allocations}}
        )
      else
        # If any function failed due to resource constraints, return error
        GenServer.reply(caller, {:error, "Resource constraints not satisfied"})
      end
    catch
      err ->
        GenServer.reply(caller, {:error, err})
    end
  end

  defp execute_functions_with_deps(dag, to_be_executed, executed, pending, max_retries) do
    if MapSet.size(to_be_executed) == 0 and MapSet.size(pending) == 0 do
      # All functions executed, return results
      executed
    else
      do_execute_functions_with_deps(dag, to_be_executed, executed, pending, max_retries)
    end
  end

  defp do_execute_functions_with_deps(dag, to_be_executed, executed, pending, max_retries) do
    # Find ready functions (all deps satisfied and not pending)
    ready_functions =
      Enum.filter(to_be_executed, fn function_id ->
        function = Map.get(dag.functions, function_id)

        # Check if all dependencies are satisfied
        all_deps_satisfied?(function, dag, executed)
      end)

    # Execute ready functions
    {new_pending, new_to_be_executed, new_executed} =
      Enum.reduce(
        ready_functions,
        {pending, to_be_executed, executed},
        &execute_ready_function(dag, max_retries, &1, &2)
      )

    # Check for completed functions if there are any pending
    if MapSet.size(new_pending) > 0 do
      # Small delay to avoid tight polling
      :timer.sleep(100 + :rand.uniform(20))

      # Check for completed functions
      {still_pending, newly_executed} =
        Enum.reduce(new_pending, {MapSet.new(), %{}}, fn function_id,
                                                         {pending_acc, executed_acc} ->
          try do
            case ResultStore.get(dag.id, function_id) do
              {:ok, result} ->
                # Function completed
                {pending_acc, Map.put(executed_acc, function_id, result)}

              {:error, :not_found} ->
                # Still pending
                {MapSet.put(pending_acc, function_id), executed_acc}

              {:error, reason} ->
                # Error occurred
                Logger.error(
                  "Error retrieving result for #{inspect(function_id)}: #{inspect(reason)}"
                )

                {pending_acc, Map.put(executed_acc, function_id, {:error, reason})}
            end
          catch
            _kind, error ->
              Logger.error(
                "Exception when checking for function #{inspect(function_id)}: #{inspect(error)}"
              )

              {pending_acc, Map.put(executed_acc, function_id, {:error, error})}
          end
        end)

      # Continue execution with updated state
      execute_functions_with_deps(
        dag,
        new_to_be_executed,
        Map.merge(new_executed, newly_executed),
        still_pending,
        max_retries
      )
    else
      # No pending functions, continue with updated state
      execute_functions_with_deps(
        dag,
        new_to_be_executed,
        new_executed,
        new_pending,
        max_retries
      )
    end
  end

  defp all_deps_satisfied?(function, dag, executed) do
    Enum.all?(function.args, fn arg_id ->
      dep_function_def = Map.get(dag.functions, arg_id)

      cond do
        is_nil(dep_function_def) ->
          # Initial literal argument
          true

        dep_function_def.type == :inline ->
          # Inline dependencies are resolved JIT by the consumer
          true

        Map.has_key?(executed, arg_id) ->
          # Regular function dependency's result is available
          true

        true ->
          # Dependency not yet satisfied
          false
      end
    end)
  end

  defp execute_ready_function(
         dag,
         max_retries,
         function_id,
         {pending_acc, to_be_executed_acc, executed_acc}
       ) do
    function = Map.get(dag.functions, function_id)

    if function.type == :inline do
      # Inline functions are executed on demand by their consumers.
      # Their actual value is not stored in the main `executed_acc` map.
      # We just remove it from the `to_be_executed` set.
      new_to_be_executed_acc = MapSet.delete(to_be_executed_acc, function_id)
      {pending_acc, new_to_be_executed_acc, executed_acc}
    else
      # Regular function execution logic
      args_for_execution =
        fetch_arguments(dag.id, function.args, executed_acc, function.node, dag.functions)

      case execute_function_on_node(
             dag.id,
             function,
             args_for_execution,
             max_retries,
             dag.functions
           ) do
        {:ok, {:remote_store_and_registry_ok, _fun_id, _node_where_stored}} ->
          executed_acc = Map.put(executed_acc, function_id, :remote_executed_and_registered)
          to_be_executed_acc = MapSet.delete(to_be_executed_acc, function_id)
          {pending_acc, to_be_executed_acc, executed_acc}

        {:ok, result} ->
          :ok = ResultStore.store(dag.id, function_id, result)
          DataLocationRegistry.register(dag.id, function_id, function.node)
          executed_acc = Map.put(executed_acc, function_id, result)
          to_be_executed_acc = MapSet.delete(to_be_executed_acc, function_id)
          {pending_acc, to_be_executed_acc, executed_acc}

        {:async, _pid} ->
          to_be_executed_acc = MapSet.delete(to_be_executed_acc, function_id)
          pending_acc = MapSet.put(pending_acc, function_id)
          {pending_acc, to_be_executed_acc, executed_acc}

        {:error, reason} ->
          Logger.error("Failed to execute function #{inspect(function_id)}: #{inspect(reason)}")
          executed_acc = Map.put(executed_acc, function_id, {:error, reason})
          to_be_executed_acc = MapSet.delete(to_be_executed_acc, function_id)
          {pending_acc, to_be_executed_acc, executed_acc}
      end
    end
  end

  defp execute_function_on_node(
         dag_id,
         function,
         args,
         max_retries,
         all_dag_functions,
         current_retry \\ 0
       ) do
    with {:ok, resources_requested?} <- maybe_request_resources(function),
         {:ok, result} <- execute_with_node_type(dag_id, function, args, all_dag_functions),
         :ok <- maybe_release_resources(function, resources_requested?) do
      {:ok, result}
    else
      {:error, :resources_unavailable} ->
        {:error,
         {:allocation_error,
          "Resources unavailable for function #{inspect(function.id)} on node #{function.node}"}}

      {:error, reason} when current_retry < max_retries ->
        Logger.warning(
          "Retrying function #{inspect(function.id)} execution (attempt #{current_retry + 1}/#{max_retries + 1}): #{inspect(reason)}"
        )

        execute_function_on_node(
          dag_id,
          function,
          args,
          max_retries,
          all_dag_functions,
          current_retry + 1
        )

      {:error, reason} ->
        raise "Failed to execute function #{inspect(function.id)} after #{max_retries + 1} attempts. Last error: #{inspect(reason)}"
    end
  rescue
    e ->
      if current_retry < max_retries do
        Logger.warning(
          "Retrying function #{inspect(function.id)} after error (attempt #{current_retry + 1}/#{max_retries + 1}): #{inspect(e)}"
        )

        execute_function_on_node(
          dag_id,
          function,
          args,
          max_retries,
          all_dag_functions,
          current_retry + 1
        )
      else
        reraise e, __STACKTRACE__
      end
  end

  defp maybe_request_resources(function) do
    if function.cost && function.node do
      case SimpleResourceTracker.request(function.node, function.cost) do
        :ok -> {:ok, true}
        error -> error
      end
    else
      {:ok, false}
    end
  end

  defp maybe_release_resources(function, resources_requested?) do
    if resources_requested? do
      SimpleResourceTracker.release(function.node, function.cost)
    end

    :ok
  end

  defp execute_with_node_type(dag_id, function, args, all_dag_functions) do
    # Local execution (node is self or nil)
    if !function.node || function.node == Node.self() do
      execute_local(function, args, all_dag_functions)
    else
      # Remote execution
      execute_remote(dag_id, function, args, all_dag_functions)
    end
  end

  defp execute_local(function, args, all_dag_functions) do
    args =
      case function.argument_inclusion do
        :variadic -> args
        :as_list -> [args]
      end

    case function.id do
      {:serialize, producer_id, consumer_id, _args} ->
        # For serializer, source_node is producer's node, target_node is consumer's node
        producer_function = Map.fetch!(all_dag_functions, producer_id)
        consumer_function = Map.fetch!(all_dag_functions, consumer_id)
        source_node = producer_function.node
        target_node = consumer_function.node

        result =
          apply_code(function.code, args ++ [source_node, target_node] ++ function.extra_args)

        {:ok, result}

      {:deserialize, producer_id, consumer_id, _args} ->
        # For deserializer, source_node is producer's node, target_node is consumer's node
        producer_function = Map.fetch!(all_dag_functions, producer_id)
        consumer_function = Map.fetch!(all_dag_functions, consumer_id)
        source_node = producer_function.node
        target_node = consumer_function.node

        result =
          apply_code(function.code, args ++ [source_node, target_node] ++ function.extra_args)

        {:ok, result}

      _ ->
        result = apply_code(function.code, args ++ function.extra_args)
        {:ok, result}
    end
  end

  defp _execute_inline_local(dag_id, inline_function_def, executed_results, all_dag_functions) do
    # Fetch arguments for the inline function itself.
    # These might also be inline or regular.
    # Note: target_node for inline's args is Node.self() as it's executing locally.
    inline_args =
      fetch_arguments(
        dag_id,
        inline_function_def.args,
        executed_results,
        # Inline functions execute on the consumer's node
        Node.self(),
        all_dag_functions
      )

    # Execute the inline function
    apply_code(inline_function_def.code, inline_args ++ inline_function_def.extra_args)
  end

  defp execute_remote(dag_id, function, args, all_dag_functions) do
    case :rpc.call(function.node, Handoff.RemoteExecutionWrapper, :execute_and_store, [
           dag_id,
           function,
           # These are arg_ids if consumer is remote
           args,
           Node.self(),
           # Pass the full map of function definitions
           all_dag_functions
         ]) do
      {:ok, :result_stored_locally} ->
        DataLocationRegistry.register(dag_id, function.id, function.node)
        {:ok, {:remote_store_and_registry_ok, function.id, function.node}}

      {:error, reason} ->
        {:error, reason}

      {:badrpc, reason} ->
        {:error, reason}
    end
  end

  # Allocate functions to nodes based on resource requirements
  defp allocate_functions(dag, node_caps, allocation_strategy) do
    # Get list of functions from the DAG, excluding inline functions
    regular_functions =
      dag.functions
      |> Map.values()
      |> Enum.filter(fn func -> func.type == :regular end)

    # Use SimpleAllocator to get node assignments for regular functions
    Handoff.SimpleAllocator.allocate(regular_functions, node_caps, allocation_strategy)
  end

  # Assign nodes to functions based on allocation result
  defp assign_nodes_to_functions(dag, allocations) do
    updated_functions =
      Enum.reduce(allocations, dag.functions, fn {function_id, node}, acc ->
        Map.update!(acc, function_id, fn function ->
          %{function | node: node}
        end)
      end)

    %{dag | functions: updated_functions}
  end

  # Sorts functions in topological order (dependencies first)
  defp topological_sort(dag) do
    {sorted, _} =
      Enum.reduce(
        Map.keys(dag.functions),
        {[], MapSet.new()},
        fn id, {sorted, visited} ->
          visit(dag, id, sorted, visited)
        end
      )

    Enum.reverse(sorted)
  end

  defp visit(dag, id, sorted, visited) do
    if MapSet.member?(visited, id) do
      {sorted, visited}
    else
      visited = MapSet.put(visited, id)

      function = Map.get(dag.functions, id)

      {sorted, visited} =
        Enum.reduce(
          function.args,
          {sorted, visited},
          fn dep_id, {sorted_acc, visited_acc} ->
            visit(dag, dep_id, sorted_acc, visited_acc)
          end
        )

      {[id | sorted], visited}
    end
  end

  # New helper function to fetch arguments from appropriate nodes
  defp fetch_arguments(dag_id, arg_ids, executed_results, target_node, all_dag_functions) do
    if target_node == Node.self() do
      # Local execution on the orchestrator node: resolve arguments.
      Enum.map(
        arg_ids,
        &resolve_argument(&1, dag_id, executed_results, target_node, all_dag_functions)
      )
    else
      # Remote execution: Orchestrator passes arg_ids.
      # RemoteExecutionWrapper on target_node will be responsible for fetching/inlining.
      arg_ids
    end
  end

  defp resolve_argument(arg_id, dag_id, executed_results, target_node, all_dag_functions) do
    # Check if it's a function_id from this dag
    function_def = Map.get(all_dag_functions, arg_id)
    result = Map.get(executed_results, arg_id)

    cond do
      function_def && function_def.type == :inline ->
        # Inline function: execute it locally (JIT) on the target_node (consumer's node)
        # Since this resolve_argument is called when target_node == Node.self(),
        # this means the consumer is local.
        _execute_inline_local(dag_id, function_def, executed_results, all_dag_functions)

      result == :remote_executed_and_registered ->
        # Find where the result is stored
        with {:ok, source_node} <- DataLocationRegistry.lookup(dag_id, arg_id),
             {:ok, actual_value} <- :rpc.call(source_node, ResultStore, :get, [dag_id, arg_id]) do
          ResultStore.store(dag_id, arg_id, actual_value)
          actual_value
        else
          {:error, :not_found} ->
            raise "No location registered for remote arg_id #{inspect(arg_id)}"

          {:error, reason} ->
            raise "Failed to fetch remote result for arg_id #{inspect(arg_id)}: #{inspect(reason)}"

          {:badrpc, reason} ->
            raise "RPC error fetching result for arg_id #{inspect(arg_id)}: #{inspect(reason)}"
        end

      result ->
        result

      true ->
        # Argument is an initial input (literal) or a regular function not yet in executed_results
        # For initial inputs, get_with_fetch will return it.
        # For regular functions, if it's not in executed_results, it implies an issue
        # unless it's an initial value not part of dag.functions.
        # The original logic of get_with_fetch is for fetching from ResultStore.
        get_with_fetch(dag_id, arg_id, target_node)
    end
  end

  defp get_with_fetch(dag_id, arg_id, target_node) do
    # Argument is an initial input or needs fetching from global ResultStore (potentially remote).
    # ResultStore.get_with_fetch will handle RPC if data isn't local to orchestrator.
    case ResultStore.get_with_fetch(dag_id, arg_id) do
      {:ok, value} ->
        value

      {:error, _reason} ->
        # Propagate error: argument not found anywhere accessible to orchestrator.
        raise RuntimeError,
          message:
            "Orchestrator failed to fetch argument #{inspect(arg_id)} for DAG #{inspect(dag_id)} for local execution on target_node #{inspect(target_node)}"
    end
  end

  defp apply_code(function, args) when is_function(function) do
    apply(function, args)
  end

  defp apply_code({module, function}, args) do
    apply(module, function, args)
  end
end
