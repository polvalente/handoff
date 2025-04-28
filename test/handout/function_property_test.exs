defmodule Handout.FunctionPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Handout.{Function, DAG}

  # Define generators for our test properties

  def id_generator do
    StreamData.one_of([
      StreamData.atom(:alphanumeric),
      StreamData.binary(min_length: 1, max_length: 10),
      StreamData.integer(1..1000)
    ])
  end

  def args_generator do
    StreamData.list_of(id_generator(), max_length: 5)
  end

  def code_generator do
    StreamData.constant(fn -> :result end)
  end

  def function_generator do
    StreamData.fixed_map(%{
      id: id_generator(),
      args: args_generator(),
      code: code_generator(),
      extra_args: StreamData.list_of(StreamData.atom(:alphanumeric), max_length: 3)
    })
    |> StreamData.map(fn attrs -> struct(Function, attrs) end)
  end

  property "adding a function to a DAG creates the expected structure" do
    check all(function <- function_generator()) do
      dag = DAG.new()
      updated_dag = DAG.add_function(dag, function)

      # The function should be stored with its id
      assert Map.has_key?(updated_dag.functions, function.id)
      assert updated_dag.functions[function.id] == function

      # Dependencies should be correctly tracked
      Enum.each(function.args, fn arg_id ->
        assert Map.has_key?(updated_dag.dependencies, arg_id)
        assert function.id in updated_dag.dependencies[arg_id]
      end)
    end
  end

  property "adding multiple functions preserves all functions" do
    check all(
            functions <- StreamData.list_of(function_generator(), min_length: 1, max_length: 10)
          ) do
      dag =
        Enum.reduce(functions, DAG.new(), fn func, dag ->
          DAG.add_function(dag, func)
        end)

      # All functions should be present
      Enum.each(functions, fn function ->
        assert Map.has_key?(dag.functions, function.id)
        assert dag.functions[function.id] == function
      end)
    end
  end
end
