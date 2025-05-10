defmodule Handoff.DAG do
  @moduledoc ~S"""
  Provides functionality for building and validating directed acyclic graphs (DAGs) of functions.

  This module is the core of the Handoff library, allowing you to:

  1. Create empty computation graphs
  2. Add functions to the graph with their dependencies
  3. Validate the graph for correctness before execution

  ## DAG Structure

  A DAG in Handoff is represented as a map with:

  * `:functions` - A map of function IDs to `Handoff.Function` structs

  ## Examples

  ```elixir
  # Create a new DAG
  dag = Handoff.DAG.new()

  # Define functions
  # Assumes MyPipelineFunctions module exists, e.g.:
  # defmodule MyPipelineFunctions do
  #   def transform_data(data_list), do: Enum.map(data_list, &(&1 * 2))
  # end

  source = %Handoff.Function{
    id: :data_source,
    args: [],
    code: &Elixir.Function.identity/1,
    extra_args: [[1, 2, 3, 4, 5]]
  }

  transform = %Handoff.Function{
    id: :transform,
    args: [:data_source],
    code: &MyPipelineFunctions.transform_data/1
  }

  aggregation = %Handoff.Function{
    id: :aggregate,
    args: [:transform],
    code: &Enum.sum/1
  }

  # Build the DAG
  dag =
    dag
    |> Handoff.DAG.add_function(source)
    |> Handoff.DAG.add_function(transform)
    |> Handoff.DAG.add_function(aggregation)

  # Validate the DAG
  case Handoff.DAG.validate(dag) do
    :ok ->
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

  alias Handoff.Function

  defstruct functions: %{}, id: nil

  @doc """
  Creates a new empty DAG with a specified ID or generates a new one.

  ## Parameters
  - id: (Optional) The ID to assign to the DAG. If nil, a `make_ref/0` will be used.

  ## Example

      dag_with_specific_id = Handoff.DAG.new("some-specific-id")
      dag_with_generated_id = Handoff.DAG.new()
  """
  def new(id \\ nil) do
    %__MODULE__{id: id || make_ref()}
  end

  @doc """
  Adds a function to the DAG.

  ## Parameters
  - dag: The current DAG structure
  - function: A Handoff.Function struct to add to the DAG

  ## Returns
  - Updated DAG with the function added

  ## Example

      dag =
        Handoff.DAG.new()
        |> Handoff.DAG.add_function(%Handoff.Function{
          id: :source,
          args: [],
          code: &:rand.uniform/1,
          extra_args: [100]
        })
  """
  def add_function(dag, %Function{} = function) do
    case function.code do
      {module, function} when is_atom(module) and is_atom(function) ->
        :ok

      code ->
        if not is_function(code) or Elixir.Function.info(code)[:type] != :external do
          raise ":code must be an fully qualified &Module.function/arity capture, got: #{inspect(code)}"
        end
    end

    # Expand Handoff.Function.Argument in args into synthetic nodes
    {dag, new_args} =
      Enum.reduce(function.args, {dag, []}, &expand_argument_nodes(&1, &2, function))

    # Replace args with rewritten args
    function = %{function | args: Enum.reverse(new_args)}
    put_in(dag.functions[function.id], function)
  end

  defp expand_argument_nodes(arg, {dag_acc, args_acc}, function) do
    case arg do
      %Handoff.Function.Argument{} = arg_spec ->
        original_producer_id = arg_spec.id
        consuming_function_id = function.id

        # 1. Serialization node
        {mod, fun, extra_args} =
          ser_mfa =
          arg_spec.serialization_fn || {Handoff.InternalOps, :identity_with_nodes, []}

        serializer_node_id =
          {:serialize, original_producer_id, consuming_function_id, ser_mfa}

        serializer_fn = %Function{
          id: serializer_node_id,
          args: [original_producer_id],
          code: {mod, fun},
          extra_args: extra_args,
          type: :regular,
          node: {:collocated, original_producer_id},
          cost: nil
        }

        dag_acc = add_function(dag_acc, serializer_fn)

        # 2. Deserialization node
        {mod, fun, extra_args} =
          deser_mfa =
          arg_spec.deserialization_fn || {Handoff.InternalOps, :identity_with_nodes, []}

        deserializer_node_id =
          {:deserialize, original_producer_id, consuming_function_id, deser_mfa}

        deserializer_fn = %Function{
          id: deserializer_node_id,
          args: [serializer_node_id],
          code: {mod, fun},
          extra_args: extra_args,
          type: :regular,
          node: {:collocated, consuming_function_id},
          cost: nil
        }

        dag_acc = add_function(dag_acc, deserializer_fn)

        {dag_acc, [deserializer_node_id | args_acc]}

      _ ->
        {dag_acc, [arg | args_acc]}
    end
  end

  @doc ~S"""
  Validates that the DAG has no cycles and all dependencies exist.

  ## Returns
  - `:ok` if the DAG is valid
  - `{:error, {:missing_function, id}}` if references to an undefined function was found
  - `{:error, {:cycle_detected, id}}` if a cycle is found in the graph

  ## Example

      iex> dag = Handoff.DAG.new()
      iex> dag = Handoff.DAG.add_function(dag, %Handoff.Function{id: :a, args: [], code: &Elixir.Function.identity/1, extra_args: [1]})
      iex> dag = Handoff.DAG.add_function(dag, %Handoff.Function{id: :b, args: [:a], code: &Map.get/2, extra_args: [:a]})
      iex> Handoff.DAG.validate(dag)
      :ok

  ## Error cases

      iex> dag = Handoff.DAG.new()
      iex> dag = Handoff.DAG.add_function(dag, %Handoff.Function{id: :a, args: [:b], code: &Elixir.Function.identity/1})
      iex> Handoff.DAG.validate(dag)
      {:error, {:missing_function, :b}}

      iex> dag = Handoff.DAG.new()
      iex> dag = Handoff.DAG.add_function(dag, %Handoff.Function{id: :a, args: [:b], code: &Map.get/2, extra_args: [:b]})
      iex> dag = Handoff.DAG.add_function(dag, %Handoff.Function{id: :b, args: [:a], code: &Map.get/2, extra_args: [:a]})
      iex> {:error, {:cyclic_dependency, cycle}} = Handoff.DAG.validate(dag)
      iex> Enum.sort(cycle)
      [:a, :b]
  """
  def validate(dag) do
    graph = :digraph.new([:acyclic])

    try do
      with :ok <- add_functions(graph, dag.functions) do
        check_for_missing_dependencies(graph)
      end
    after
      :digraph.delete(graph)
    end
  end

  defp add_functions(graph, functions) do
    # Add vertices and edges
    Enum.reduce_while(functions, :ok, fn {id, function}, _acc ->
      with :ok <- validate_definition(function),
           :ok <- add_vertex(graph, id),
           :ok <- add_dependencies(graph, function) do
        {:cont, :ok}
      else
        error ->
          {:halt, error}
      end
    end)
  end

  defp validate_definition(function) do
    if function.type == :inline and not is_nil(function.node) do
      {:error, {:invalid_inline_function_node, function.id}}
    else
      :ok
    end
  end

  defp check_for_missing_dependencies(graph) do
    # Check for missing dependencies (vertices with label nil)
    missing =
      Enum.find_value(:digraph.vertices(graph), fn v ->
        case :digraph.vertex(graph, v) do
          {^v, nil} -> v
          _ -> false
        end
      end)

    if missing do
      {:error, {:missing_function, missing}}
    else
      :ok
    end
  end

  defp add_vertex(graph, id) do
    # Add or update vertex for the function itself
    case :digraph.vertex(graph, id) do
      false ->
        :digraph.add_vertex(graph, id, id)
        :ok

      {^id, nil} ->
        :digraph.add_vertex(graph, id, id)
        :ok

      {^id, _} ->
        {:error, {:duplicate_function, id}}
    end
  end

  defp add_dependencies(graph, function) do
    # Add vertices for dependencies (label nil if not present)
    Enum.reduce_while(function.args, :ok, fn dep_id, _acc ->
      case :digraph.vertex(graph, dep_id) do
        false ->
          :digraph.add_vertex(graph, dep_id, nil)
          :ok

        _ ->
          :ok
      end

      # TO-DO: return error when add_edge fails because the graph is cyclic
      case :digraph.add_edge(graph, dep_id, function.id) do
        {:error, _} ->
          {:halt, {:error, {:cyclic_dependency, [dep_id, function.id]}}}

        _ ->
          {:cont, :ok}
      end
    end)
  end
end
