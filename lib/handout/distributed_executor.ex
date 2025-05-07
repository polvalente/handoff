defmodule Handout.DistributedExecutor do
  @moduledoc """
  Handles the distributed execution of functions across multiple nodes.

  Provides node discovery, coordination, and remote task execution capabilities.
  """

  use GenServer
  require Logger
  alias Handout.{DAG, ResultStore, SimpleResourceTracker, DataLocationRegistry}

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
    # Clear any previous results for this DAG
    ResultStore.clear(dag.id)

    # Clear data location registry for this DAG
    DataLocationRegistry.clear(dag.id)

    # Register initial arguments in the data location registry for this DAG
    dag.functions
    |> Map.values()
    |> Enum.each(fn function ->
      # For functions with no arguments (source functions), we don't need to register anything
      if function.args == [] or function.args == nil do
        :ok
      else
        # Register each initial argument as available on the local node for this DAG
        function.args
        |> Enum.each(fn arg_id ->
          # Only register if it's a literal value, not a function ID
          unless Map.has_key?(dag.functions, arg_id) do
            DataLocationRegistry.register(dag.id, arg_id, Node.self())
          end
        end)
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
      # Find ready functions (all deps satisfied and not pending)
      ready_functions =
        Enum.filter(to_be_executed, fn function_id ->
          function = Map.get(dag.functions, function_id)

          # Check if all dependencies are executed
          all_deps_executed? =
            Enum.all?(function.args, fn arg_id ->
              # An argument is considered executed if it's in the executed map
              # OR if it's an initial argument (not a function_id from this dag).
              # For simplicity, we assume initial args are always "ready".
              # Proper handling of initial args vs. function results would be more robust here.
              Map.has_key?(executed, arg_id) or not Map.has_key?(dag.functions, arg_id)
            end)

          all_deps_executed?
        end)

      # Execute ready functions
      {new_pending, new_to_be_executed, new_executed} =
        Enum.reduce(ready_functions, {pending, to_be_executed, executed}, fn function_id,
                                                                             {pending_acc,
                                                                              to_be_executed_acc,
                                                                              executed_acc} ->
          function = Map.get(dag.functions, function_id)

          # Prepare arguments. For remote execution, this will pass arg_ids.
          # For local, it resolves them.
          args_for_execution = fetch_arguments(dag.id, function.args, executed_acc, function.node)

          # Execute the function on the assigned node
          Logger.debug(
            "Executing function #{function_id} on node #{inspect(function.node)} with args: #{inspect(args_for_execution)}"
          )

          try do
            # Pass `args_for_execution` which are actual values for local, or arg_ids for remote.
            case execute_function_on_node(dag.id, function, args_for_execution, max_retries) do
              {:ok, {:remote_store_and_registry_ok, _fun_id, _node_where_stored}} ->
                # For remote execution, the result is not returned directly.
                # We mark it as executed by adding its ID with a placeholder or status.
                # The actual result can be fetched via Handout.get_result if needed later.
                executed_acc = Map.put(executed_acc, function_id, :remote_executed_and_registered)
                to_be_executed_acc = MapSet.delete(to_be_executed_acc, function_id)
                {pending_acc, to_be_executed_acc, executed_acc}

              # This case is for local execution where result is returned directly
              {:ok, result} ->
                # Store result locally
                :ok = ResultStore.store(dag.id, function_id, result)

                # Register result location in the registry
                DataLocationRegistry.register(dag.id, function_id, function.node)

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
              case ResultStore.get(dag.id, function_id) do
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

  defp execute_function_on_node(dag_id, function, args, max_retries, current_retry \\ 0) do
    # Request resources if function has a cost
    # No function.cost or no function.node (should imply local execution or error in allocation)
    if function.cost && function.node do
      case SimpleResourceTracker.request(function.node, function.cost) do
        :ok ->
          try do
            # For local node execution
            if function.node == Node.self() do
              result = apply(function.code, args ++ function.extra_args)
              # Release resources
              SimpleResourceTracker.release(function.node, function.cost)
              # Local execution still returns the result directly
              {:ok, result}
            else
              # For remote node execution - the orchestrator is this node (self)
              case :rpc.call(function.node, Handout.RemoteExecutionWrapper, :execute_and_store, [
                     dag_id,
                     function,
                     args,
                     # Pass the orchestrator node explicitly
                     Node.self()
                   ]) do
                {:ok, :result_stored_locally} ->
                  SimpleResourceTracker.release(function.node, function.cost)
                  DataLocationRegistry.register(dag_id, function.id, function.node)
                  # Signal success; actual data is not returned from remote node directly
                  {:ok, {:remote_store_and_registry_ok, function.id, function.node}}

                {:error, remote_error_detail} ->
                  SimpleResourceTracker.release(function.node, function.cost)

                  Logger.warning(
                    "Remote execution or store failed for function #{function.id} (attempt #{current_retry + 1}/#{max_retries + 1}): #{inspect(remote_error_detail)}"
                  )

                  if current_retry < max_retries do
                    execute_function_on_node(
                      dag_id,
                      function,
                      args,
                      max_retries,
                      current_retry + 1
                    )
                  else
                    # Ensure to raise a distinct error for propagation
                    raise %RuntimeError{
                      message:
                        "Failed to execute function #{function.id} on node #{function.node} after #{max_retries + 1} attempts. Last error: #{inspect(remote_error_detail)}"
                    }
                  end

                {:badrpc, reason} ->
                  # Release resources
                  SimpleResourceTracker.release(function.node, function.cost)

                  if current_retry < max_retries do
                    # Retry execution
                    Logger.warning(
                      "Retrying function #{function.id} execution after RPC error (attempt #{current_retry + 1}/#{max_retries + 1}): #{inspect(reason)}"
                    )

                    execute_function_on_node(
                      dag_id,
                      function,
                      args,
                      max_retries,
                      current_retry + 1
                    )
                  else
                    raise %RuntimeError{
                      message:
                        "Failed to execute function #{function.id} via RPC on node #{function.node} after #{max_retries + 1} attempts: #{inspect(reason)}"
                    }
                  end
              end
            end
          rescue
            # This rescue is for errors in the local node's logic BEFORE or AFTER rpc, or local execution
            e ->
              # Release resources on error
              SimpleResourceTracker.release(function.node, function.cost)

              if current_retry < max_retries do
                Logger.warning(
                  "Retrying function #{function.id} (orchestrator error or local exec error, attempt #{current_retry + 1}/#{max_retries + 1}): #{inspect(e)}"
                )

                execute_function_on_node(dag_id, function, args, max_retries, current_retry + 1)
              else
                raise %RuntimeError{
                  message:
                    "Failed to process function #{function.id} after #{max_retries + 1} attempts (orchestrator/local error): #{inspect(e)}"
                }
              end
          end

        {:error, :resources_unavailable} ->
          # Return an error instead of raising, if resources couldn't be acquired initially
          # This indicates the SimpleResourceTracker denied the request for function.node
          {:error,
           %RuntimeError{
             message:
               "Resources unavailable for function #{function.id} on node #{function.node} (request by orchestrator)"
           }}
      end
    else
      # No resource requirements, just execute (assumed local or pre-validated)
      # If function.node is nil here but code is remote, that's a logical error upstream.
      # Assuming if function.node is nil, it must run locally.
      try do
        # If function.node is not specified, assume local execution.
        # If function.node IS specified but no cost, it respects the node if remote.
        # Local execution (no cost or no specific node)
        if function.node && function.node != Node.self() do
          # Remote execution without cost - the orchestrator is this node (self)
          case :rpc.call(function.node, Handout.RemoteExecutionWrapper, :execute_and_store, [
                 dag_id,
                 function,
                 args,
                 # Pass the orchestrator node explicitly
                 Node.self()
               ]) do
            {:ok, :result_stored_locally} ->
              DataLocationRegistry.register(dag_id, function.id, function.node)
              {:ok, {:remote_store_and_registry_ok, function.id, function.node}}

            {:error, remote_error_detail} ->
              Logger.warning(
                "Remote execution (no cost) or store failed for function #{function.id} (attempt #{current_retry + 1}/#{max_retries + 1}): #{inspect(remote_error_detail)}"
              )

              if current_retry < max_retries do
                execute_function_on_node(dag_id, function, args, max_retries, current_retry + 1)
              else
                raise %RuntimeError{
                  message:
                    "Failed to execute function #{function.id} (no cost) on node #{function.node} after #{max_retries + 1} attempts. Last error: #{inspect(remote_error_detail)}"
                }
              end

            {:badrpc, reason} ->
              if current_retry < max_retries do
                Logger.warning(
                  "Retrying function #{function.id} (no cost) execution after RPC error (attempt #{current_retry + 1}/#{max_retries + 1}): #{inspect(reason)}"
                )

                execute_function_on_node(dag_id, function, args, max_retries, current_retry + 1)
              else
                raise %RuntimeError{
                  message:
                    "Failed to execute function #{function.id} (no cost) via RPC on node #{function.node} after #{max_retries + 1} attempts: #{inspect(reason)}"
                }
              end
          end
        else
          result = apply(function.code, args ++ function.extra_args)
          {:ok, result}
        end
      rescue
        e ->
          if current_retry < max_retries do
            Logger.warning(
              "Retrying function #{function.id} (no cost, local error, attempt #{current_retry + 1}/#{max_retries + 1}): #{inspect(e)}"
            )

            execute_function_on_node(dag_id, function, args, max_retries, current_retry + 1)
          else
            raise %RuntimeError{
              message:
                "Failed to execute function #{function.id} (no cost, local) after #{max_retries + 1} attempts: #{inspect(e)}"
            }
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

  # New helper function to fetch arguments from appropriate nodes
  defp fetch_arguments(dag_id, arg_ids, executed_results, target_node) do
    if target_node == Node.self() do
      # Local execution on the orchestrator node: resolve arguments as before.
      Enum.map(arg_ids, fn arg_id ->
        if Map.has_key?(executed_results, arg_id) do
          # Argument is a result from a previously executed function in this DAG run
          result = Map.get(executed_results, arg_id)

          # If it's a remote execution marker, fetch the actual value
          if result == :remote_executed_and_registered do
            # Find where the result is stored
            case DataLocationRegistry.lookup(dag_id, arg_id) do
              {:ok, source_node} ->
                # Fetch from the node where it's stored
                case :rpc.call(source_node, ResultStore, :get, [dag_id, arg_id]) do
                  {:ok, actual_value} ->
                    # Store locally for future use
                    ResultStore.store(dag_id, arg_id, actual_value)
                    actual_value

                  {:error, reason} ->
                    raise "Failed to fetch remote result for arg_id #{inspect(arg_id)}: #{inspect(reason)}"

                  {:badrpc, reason} ->
                    raise "RPC error fetching result for arg_id #{inspect(arg_id)}: #{inspect(reason)}"
                end

              {:error, :not_found} ->
                raise "No location registered for remote arg_id #{inspect(arg_id)}"
            end
          else
            # Return the actual result already present
            result
          end
        else
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
      end)
    else
      # Remote execution: Orchestrator passes arg_ids.
      # RemoteExecutionWrapper on target_node will be responsible for fetching them from its *local* ResultStore.
      arg_ids
    end
  end
end
