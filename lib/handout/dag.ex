defmodule Handout.DAG do
  @moduledoc """
  Provides functionality for building and validating directed acyclic graphs (DAGs) of functions.
  """

  alias Handout.Function

  @doc """
  Creates a new empty DAG.
  """
  def new do
    %{
      functions: %{},
      dependencies: %{}
    }
  end

  @doc """
  Adds a function to the DAG.

  ## Parameters
  - dag: The current DAG structure
  - function: A Handout.Function struct to add to the DAG

  ## Returns
  - Updated DAG with the function added
  """
  def add_function(dag, %Function{} = function) do
    updated_functions = Map.put(dag.functions, function.id, function)

    # Update dependency tracking
    updated_dependencies =
      Enum.reduce(function.args, dag.dependencies, fn arg_id, deps ->
        Map.update(deps, arg_id, [function.id], fn dependents ->
          [function.id | dependents] |> Enum.uniq()
        end)
      end)

    %{dag | functions: updated_functions, dependencies: updated_dependencies}
  end

  @doc """
  Validates that the DAG has no cycles and all dependencies exist.

  ## Parameters
  - dag: The DAG to validate

  ## Returns
  - {:ok, dag} if the DAG is valid
  - {:error, reason} if the DAG is invalid
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
