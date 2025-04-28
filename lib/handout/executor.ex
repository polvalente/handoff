defmodule Handout.Executor do
  @moduledoc """
  Executes DAG functions in dependency order.

  Handles scheduling, dependency resolution, and function execution.
  """

  use GenServer
  alias Handout.{DAG, ResultStore}

  # Client API

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Executes all functions in a validated DAG.

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
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:execute, dag, _opts}, _from, state) do
    # Clear any previous results
    ResultStore.clear()

    # Execute functions in topological order
    topo_sorted = topological_sort(dag)

    # Execute each function in the sorted order
    results =
      Enum.reduce_while(topo_sorted, %{}, fn function_id, results_acc ->
        function = Map.get(dag.functions, function_id)

        case execute_function(function, results_acc) do
          {:ok, result} ->
            # Store result in ETS and accumulator
            ResultStore.store(function_id, result)
            {:cont, Map.put(results_acc, function_id, result)}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end)

    case results do
      {:error, _} = error -> {:reply, error, state}
      results -> {:reply, {:ok, results}, state}
    end
  end

  # Private functions

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

  # Executes a single function, resolving its dependencies
  defp execute_function(function, results_acc) do
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
