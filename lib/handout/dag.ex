defmodule Handout.DAG do
  @moduledoc ~S"""
  Provides functionality for building and validating directed acyclic graphs (DAGs) of functions.

  This module is the core of the Handout library, allowing you to:

  1. Create empty computation graphs
  2. Add functions to the graph with their dependencies
  3. Validate the graph for correctness before execution

  ## DAG Structure

  A DAG in Handout is represented as a map with:

  * `:functions` - A map of function IDs to `Handout.Function` structs

  ## Examples

  ```elixir
  # Create a new DAG
  dag = Handout.DAG.new()

  # Define functions
  source = %Handout.Function{
    id: :data_source,
    args: [],
    code: fn -> [1, 2, 3, 4, 5] end
  }

  transform = %Handout.Function{
    id: :transform,
    args: [:data_source],
    code: fn %{data_source: data} -> Enum.map(data, &(&1 * 2)) end
  }

  aggregation = %Handout.Function{
    id: :aggregate,
    args: [:transform],
    code: fn %{transform: data} -> Enum.sum(data) end
  }

  # Build the DAG
  dag =
    dag
    |> Handout.DAG.add_function(source)
    |> Handout.DAG.add_function(transform)
    |> Handout.DAG.add_function(aggregation)

  # Validate the DAG
  case Handout.DAG.validate(dag) do
    {:ok, validated_dag} ->
      # DAG is valid and ready for execution
      IO.puts("DAG is valid")

    {:error, {:missing_dependencies, missing}} ->
      IO.puts("DAG has missing dependencies: #{inspect(missing)}")

    {:error, {:cycle_detected, cycle}} ->
      IO.puts("DAG contains a cycle at: #{inspect(cycle)}")
  end
  ```

  ## Validation

  The `validate/1` function performs two critical checks:

  1. It ensures all dependencies reference existing functions
  2. It detects cycles in the graph using depth-first search

  A valid DAG is required before execution.
  """

  alias Handout.Function

  defstruct functions: %{}

  @doc """
  Creates a new empty DAG.

  Returns a map with empty `:functions` map that can be
  populated using `add_function/2`.

  ## Example

  ```elixir
  dag = Handout.DAG.new()
  ```
  """
  def new do
    %__MODULE__{}
  end

  @doc """
  Adds a function to the DAG.

  ## Parameters
  - dag: The current DAG structure
  - function: A Handout.Function struct to add to the DAG

  ## Returns
  - Updated DAG with the function added

  ## Example

  ```elixir
  dag =
    Handout.DAG.new()
    |> Handout.DAG.add_function(%Handout.Function{
      id: :source,
      args: [],
      code: fn -> :rand.uniform(100) end
    })
  ```
  """
  def add_function(dag, %Function{} = function) do
    put_in(dag.functions[function.id], function)
  end

  @doc ~S"""
  Validates that the DAG has no cycles and all dependencies exist.

  ## Parameters
  - dag: The DAG to validate

  ## Returns
  - {:ok, dag} if the DAG is valid
  - {:error, {:missing_dependencies, list}} if references to non-existent functions exist
  - {:error, {:cycle_detected, id}} if a cycle is found in the graph

  ## Example

  ```elixir
  case Handout.DAG.validate(dag) do
    {:ok, validated_dag} ->
      # DAG is valid and ready for execution
      Handout.execute(validated_dag)

    {:error, reason} ->
      # Handle the validation error
      IO.puts("DAG validation failed: #{inspect(reason)}")
  end
  ```
  """
  def validate(dag) do
    with :ok <- validate_dependencies_exist(dag),
         :ok <- validate_no_cycles(dag) do
      {:ok, dag}
    end
  end

  # Ensure all dependencies reference existing functions
  defp validate_dependencies_exist(dag) do
    all_function_ids = Map.keys(dag.functions)

    missing_deps =
      dag.functions
      |> Enum.flat_map(fn {_, function} -> function.args end)
      |> Enum.uniq()
      |> Enum.reject(&(&1 in all_function_ids))

    case missing_deps do
      [] -> :ok
      missing -> {:error, {:missing_dependencies, missing}}
    end
  end

  # Check for cycles in the graph
  defp validate_no_cycles(dag) do
    visited = MapSet.new()
    temp_visited = MapSet.new()

    function_ids = Map.keys(dag.functions)

    try do
      {_, _} =
        Enum.reduce(function_ids, {visited, temp_visited}, fn id, {visited, temp_visited} ->
          if MapSet.member?(visited, id) do
            {visited, temp_visited}
          else
            dfs(dag, id, visited, temp_visited)
          end
        end)

      :ok
    catch
      {:cycle_detected, cycle} -> {:error, {:cycle_detected, cycle}}
    end
  end

  # Depth-first search for cycle detection
  defp dfs(dag, id, visited, temp_visited) do
    if MapSet.member?(temp_visited, id) do
      throw({:cycle_detected, id})
    end

    if MapSet.member?(visited, id) do
      {visited, temp_visited}
    else
      temp_visited = MapSet.put(temp_visited, id)

      function = Map.get(dag.functions, id)
      dependencies = function.args

      {visited, temp_visited} =
        Enum.reduce(dependencies, {visited, temp_visited}, fn dep_id, {v, tv} ->
          dfs(dag, dep_id, v, tv)
        end)

      {MapSet.put(visited, id), MapSet.delete(temp_visited, id)}
    end
  end
end
