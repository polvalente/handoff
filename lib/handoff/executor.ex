defmodule Handoff.Executor do
  @moduledoc """
  Executes DAG functions in dependency order.

  Handles scheduling, dependency resolution, and function execution.
  """

  use GenServer

  alias Handoff.DAG
  alias Handoff.ResultStore
  alias Handoff.SimpleAllocator
  alias Handoff.SimpleResourceTracker

  # Client API

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Executes all functions in a validated DAG.

  ## Parameters
  - dag: A validated DAG to execute
  - opts: Optional execution options
    - :allocation_strategy - Strategy for allocating functions to nodes
      (:first_available or :load_balanced, defaults to :first_available)

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
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:execute, dag, opts}, _from, state) do
    # Clear any previous results for this DAG
    ResultStore.clear(dag.id)

    # Get node capabilities
    # For now, we're just using the local node
    node_caps = %{Node.self() => %{cpu: 8, memory: 4000}}

    # Allocate functions to nodes
    allocation_strategy = Keyword.get(opts, :allocation_strategy, :first_available)
    allocations = allocate_functions(dag, node_caps, allocation_strategy)

    # Update dag functions with node assignments
    dag = assign_nodes_to_functions(dag, allocations)

    # Execute functions in topological order
    topo_sorted = topological_sort(dag)

    # Execute each function in the sorted order
    results =
      Enum.reduce_while(topo_sorted, %{}, fn function_id, results_acc ->
        function = Map.get(dag.functions, function_id)

        case execute_function(function, results_acc) do
          {:ok, result} ->
            # Release resources if function has a cost
            if function.cost && function.node do
              SimpleResourceTracker.release(function.node, function.cost)
            end

            # Store result in ETS and accumulator
            ResultStore.store(dag.id, function_id, result)
            {:cont, Map.put(results_acc, function_id, result)}

          {:error, reason} ->
            # Release resources on error too
            if function.cost && function.node do
              SimpleResourceTracker.release(function.node, function.cost)
            end

            {:halt, {:error, reason}}
        end
      end)

    case results do
      {:error, _} = error -> {:reply, error, state}
      results -> {:reply, {:ok, %{dag_id: dag.id, results: results}}, state}
    end
  end

  # Private functions

  # Allocate functions to nodes based on resource requirements
  defp allocate_functions(dag, node_caps, allocation_strategy) do
    # Get list of functions from the DAG
    functions = Map.values(dag.functions)

    # Use allocator to get node assignments
    SimpleAllocator.allocate(functions, node_caps, allocation_strategy)
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

  # Executes a single function, resolving its dependencies
  defp execute_function(function, results_acc) do
    # Request resources if function has a cost
    if function.cost && function.node do
      case SimpleResourceTracker.request(function.node, function.cost) do
        :ok ->
          # Resources allocated, continue
          :ok

        {:error, :resources_unavailable} ->
          # Could not allocate resources
          {:error, {:resources_unavailable, function.id, function.node}}
      end
    end

    # Get argument values from results
    args =
      Enum.map(function.args, fn arg_id ->
        Map.get(results_acc, arg_id)
      end)

    # Execute the function with arguments and extra_args
    try do
      result = apply(function.code, args ++ function.extra_args)
      {:ok, result}
    rescue
      e -> {:error, {e, __STACKTRACE__}}
    end
  end
end
