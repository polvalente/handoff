defmodule Handoff.ExecutorTest do
  use ExUnit.Case, async: false

  alias Handoff.DAG
  alias Handoff.Executor
  alias Handoff.Function
  alias Handoff.ResultStore

  describe "single-node execution" do
    test "executes a simple DAG in dependency order" do
      # Create a simple DAG: A -> B -> C
      # Where A = 1, B = A + 1, C = B * 2

      dag = Handoff.DAG.new()

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :a,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [1]
        })
        |> DAG.add_function(%Function{
          id: :b,
          args: [:a],
          code: &Kernel.+/2,
          extra_args: [1]
        })
        |> DAG.add_function(%Function{
          id: :c,
          args: [:b],
          code: &Kernel.*/2,
          extra_args: [2]
        })

      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               Executor.execute(dag_with_functions)

      assert returned_dag_id == dag.id
      assert actual_results[:a] == 1
      assert actual_results[:b] == 2
      assert actual_results[:c] == 4

      # Check results are in ResultStore for the correct DAG ID
      assert {:ok, 1} = ResultStore.get(dag.id, :a)
      assert {:ok, 2} = ResultStore.get(dag.id, :b)
      assert {:ok, 4} = ResultStore.get(dag.id, :c)
    end

    test "executes a DAG with multiple dependencies" do
      # Create a diamond DAG: A -> B -> D
      #                       \-> C -/

      dag = Handoff.DAG.new()

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :a,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [5]
        })
        |> DAG.add_function(%Function{
          id: :b,
          args: [:a],
          code: &Kernel.+/2,
          extra_args: [2]
        })
        |> DAG.add_function(%Function{
          id: :c,
          args: [:a],
          code: &Kernel.*/2,
          extra_args: [2]
        })
        |> DAG.add_function(%Function{
          id: :d,
          args: [:b, :c],
          code: &Kernel.+/2,
        })

      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               Executor.execute(dag_with_functions)

      assert returned_dag_id == dag.id
      assert actual_results[:a] == 5
      assert actual_results[:b] == 7
      assert actual_results[:c] == 10
      assert actual_results[:d] == 17
    end

    test "handles a DAG with extra_args" do
      dag = Handoff.DAG.new()

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :a,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [1]
        })
        |> DAG.add_function(%Function{
          id: :b,
          args: [:a],
          code: &Kernel.*/2,
          extra_args: [10]
        })

      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               Executor.execute(dag_with_functions)

      assert returned_dag_id == dag.id
      assert actual_results[:a] == 1
      assert actual_results[:b] == 10
    end

    test "handles execution errors" do
      dag = Handoff.DAG.new()

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :a,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [1]
        })
        |> DAG.add_function(%Function{
          id: :error,
          args: [:a],
          code: &Handoff.DistributedTestFunctions.raise_error/2,
          extra_args: ["An error occurred"]
        })

      {:error, error_info} = Executor.execute(dag_with_functions)
      assert match?({%RuntimeError{message: "An error occurred, value: 1"}, _}, error_info)
    end

    test "rejects invalid DAGs" do
      # Create a cyclic DAG: A -> B -> A
      dag = Handoff.DAG.new()

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :a,
          args: [:b],
          code: &Kernel.+/2,
          extra_args: [1]
        })
        |> DAG.add_function(%Function{
          id: :b,
          args: [:a],
          code: &Kernel.*/2,
          extra_args: [2]
        })

      assert {:error, {:cyclic_dependency, cycle}} = Executor.execute(dag_with_functions)
      assert Enum.sort(cycle) == [:a, :b]
    end

    @tag :skip
    test "can fetch a single element from an argument" do
      dag = Handoff.DAG.new()

      execute_result =
        dag
        |> DAG.add_function(%Function{id: :arg0, args: [], code: fn -> [1, 2, 3] end})
        |> DAG.add_function(%Function{id: :arg1, args: [], code: fn -> [[10, 20, 30]] end})
        |> DAG.add_function(%Function{
          id: :zip,
          args: [{:fetch, :arg0, 0}, {:fetch, :arg0, 1}, {:fetch, :arg1, 0}],
          code: fn arg00, arg01, arg10, offset ->
            Enum.map(arg10, fn x -> x + arg00 + arg01 + offset end)
          end,
          extra_args: [offset: 10]
        })
        |> Handoff.execute()

      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} = execute_result
      assert returned_dag_id == dag.id
      assert actual_results == %{arg0: [1, 2, 3], arg1: [[10, 20, 30]], zip: [23, 33, 43]}
    end
  end
end
