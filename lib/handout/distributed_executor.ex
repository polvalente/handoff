defmodule Handout.DistributedExecutor do
  @moduledoc """
  Handles the distributed execution of functions across multiple nodes.

  Provides node discovery, coordination, and remote task execution capabilities.
  """

  use GenServer
  require Logger
  alias Handout.{DAG, ResultStore, SimpleResourceTracker}

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
      {:ok, _} -> GenServer.call(__MODULE__, {:execute, dag, opts}, :infinity)
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
      [Node.self() | Node.list()]
      |> Enum.reduce(%{}, fn node, acc ->
        case :rpc.call(node, Handout.SimpleResourceTracker, :get_capabilities, []) do
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
    # Clear any previous results
    ResultStore.clear()

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
    spawn_link(fn ->
      execute_dag(dag, opts, from, state.max_retries)
    end)

    {:noreply, state}
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
        Logger.warning("Function #{function_id} execution failed: #{inspect(reason)}")

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
        case :rpc.call(node, Handout.SimpleResourceTracker, :get_capabilities, []) do
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
        GenServer.reply(caller, {:ok, results})
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
      # Find ready functions (all deps satisfied and not pending)
      ready_functions =
        Enum.filter(to_be_executed, fn function_id ->
          function = Map.get(dag.functions, function_id)

          # Check if all dependencies are executed
          all_deps_executed? =
            Enum.all?(function.args, fn arg_id -> Map.has_key?(executed, arg_id) end)

          all_deps_executed?
        end)

      # Execute ready functions
      {new_pending, new_to_be_executed, new_executed} =
        Enum.reduce(ready_functions, {pending, to_be_executed, executed}, fn function_id,
                                                                             {pending_acc,
                                                                              to_be_executed_acc,
                                                                              executed_acc} ->
          function = Map.get(dag.functions, function_id)

          # Prepare arguments from executed results
          args = Enum.map(function.args, fn arg_id -> Map.get(executed_acc, arg_id) end)

          # Execute the function on the assigned node
          Logger.debug("Executing function #{function_id} on node #{inspect(function.node)}")

          try do
            case execute_function_on_node(function, args, max_retries) do
              {:ok, result} ->
                # Store result and continue
                :ok = ResultStore.store(function_id, result)
                # Add to executed and remove from to_be_executed
                executed_acc = Map.put(executed_acc, function_id, result)
                to_be_executed_acc = MapSet.delete(to_be_executed_acc, function_id)
                {pending_acc, to_be_executed_acc, executed_acc}

              {:async, _pid} ->
                # Function is executing asynchronously
                # Add to pending and remove from to_be_executed
                to_be_executed_acc = MapSet.delete(to_be_executed_acc, function_id)
                pending_acc = MapSet.put(pending_acc, function_id)
                {pending_acc, to_be_executed_acc, executed_acc}

              {:error, reason} ->
                Logger.error("Failed to execute function #{function_id}: #{inspect(reason)}")
                executed_acc = Map.put(executed_acc, function_id, {:error, reason})
                to_be_executed_acc = MapSet.delete(to_be_executed_acc, function_id)
                {pending_acc, to_be_executed_acc, executed_acc}
            end
          catch
            _kind, error ->
              Logger.error("Exception when executing function #{function_id}: #{inspect(error)}")
              executed_acc = Map.put(executed_acc, function_id, {:error, error})
              to_be_executed_acc = MapSet.delete(to_be_executed_acc, function_id)
              {pending_acc, to_be_executed_acc, executed_acc}
          end
        end)

      # Check for completed functions if there are any pending
      if MapSet.size(new_pending) > 0 do
        # Small delay to avoid tight polling
        :timer.sleep(100)

        # Check for completed functions
        {still_pending, newly_executed} =
          Enum.reduce(new_pending, {MapSet.new(), %{}}, fn function_id,
                                                           {pending_acc, executed_acc} ->
            try do
              case ResultStore.get(function_id) do
                {:ok, result} ->
                  # Function completed
                  {pending_acc, Map.put(executed_acc, function_id, result)}

                {:error, :not_found} ->
                  # Still pending
                  {MapSet.put(pending_acc, function_id), executed_acc}

                {:error, reason} ->
                  # Error occurred
                  Logger.error("Error retrieving result for #{function_id}: #{inspect(reason)}")
                  {pending_acc, Map.put(executed_acc, function_id, {:error, reason})}
              end
            catch
              _kind, error ->
                Logger.error(
                  "Exception when checking for function #{function_id}: #{inspect(error)}"
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
  end

  defp execute_function_on_node(function, args, max_retries, current_retry \\ 0) do
    # Request resources if function has a cost
    if function.cost && function.node do
      case SimpleResourceTracker.request(function.node, function.cost) do
        :ok ->
          try do
            # For local node execution
            if function.node == Node.self() do
              result = apply(function.code, args ++ function.extra_args)
              # Release resources
              SimpleResourceTracker.release(function.node, function.cost)
              {:ok, result}
            else
              # For remote node execution
              case :rpc.call(function.node, Kernel, :apply, [
                     function.code,
                     args ++ function.extra_args
                   ]) do
                {:badrpc, reason} ->
                  # Release resources
                  SimpleResourceTracker.release(function.node, function.cost)

                  if current_retry < max_retries do
                    # Retry execution
                    Logger.warning(
                      "Retrying function #{function.id} execution after error: #{inspect(reason)}"
                    )

                    execute_function_on_node(function, args, max_retries, current_retry + 1)
                  else
                    raise "Failed to execute function #{function.id} after #{max_retries} retries: #{inspect(reason)}"
                  end

                result ->
                  # Release resources
                  SimpleResourceTracker.release(function.node, function.cost)
                  {:ok, result}
              end
            end
          rescue
            e ->
              # Release resources on error
              SimpleResourceTracker.release(function.node, function.cost)

              if current_retry < max_retries do
                # Retry execution
                Logger.warning(
                  "Retrying function #{function.id} execution after error: #{inspect(e)}"
                )

                execute_function_on_node(function, args, max_retries, current_retry + 1)
              else
                raise "Failed to execute function #{function.id} after #{max_retries} retries: #{inspect(e)}"
              end
          end

        {:error, :resources_unavailable} ->
          # Return an error instead of raising
          {:error,
           %RuntimeError{
             message: "Resources unavailable for function #{function.id} on node #{function.node}"
           }}
      end
    else
      # No resource requirements, just execute
      try do
        if function.node == Node.self() do
          result = apply(function.code, args ++ function.extra_args)
          {:ok, result}
        else
          case :rpc.call(function.node, Kernel, :apply, [
                 function.code,
                 args ++ function.extra_args
               ]) do
            {:badrpc, reason} ->
              if current_retry < max_retries do
                execute_function_on_node(function, args, max_retries, current_retry + 1)
              else
                raise "Failed to execute function #{function.id} after #{max_retries} retries: #{inspect(reason)}"
              end

            result ->
              {:ok, result}
          end
        end
      rescue
        e ->
          if current_retry < max_retries do
            execute_function_on_node(function, args, max_retries, current_retry + 1)
          else
            raise "Failed to execute function #{function.id} after #{max_retries} retries: #{inspect(e)}"
          end
      end
    end
  end

  # Allocate functions to nodes based on resource requirements
  defp allocate_functions(dag, node_caps, allocation_strategy) do
    # Get list of functions from the DAG
    functions = Map.values(dag.functions)

    # Use SimpleAllocator to get node assignments
    Handout.SimpleAllocator.allocate(functions, node_caps, allocation_strategy)
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
    cond do
      MapSet.member?(visited, id) ->
        {sorted, visited}

      true ->
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
end
