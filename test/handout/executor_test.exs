defmodule Handout.ExecutorTest do
  use ExUnit.Case, async: true
  alias Handout.{Function, DAG, Executor, ResultStore}

  describe "single-node execution" do
    test "executes a simple DAG in dependency order" do
      # Create a simple DAG: A -> B -> C
      # Where A = 1, B = A + 1, C = B * 2

      dag = Handout.DAG.new()

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :a,
          args: [],
          code: fn -> 1 end
        })
        |> DAG.add_function(%Function{
          id: :b,
          args: [:a],
          code: fn a -> a + 1 end
        })
        |> DAG.add_function(%Function{
          id: :c,
          args: [:b],
          code: fn b -> b * 2 end
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

      dag = Handout.DAG.new()

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :a,
          args: [],
          code: fn -> 5 end
        })
        |> DAG.add_function(%Function{
          id: :b,
          args: [:a],
          code: fn a -> a + 2 end
        })
        |> DAG.add_function(%Function{
          id: :c,
          args: [:a],
          code: fn a -> a * 2 end
        })
        |> DAG.add_function(%Function{
          id: :d,
          args: [:b, :c],
          code: fn b, c -> b + c end
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
      dag = Handout.DAG.new()

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :a,
          args: [],
          code: fn -> 1 end
        })
        |> DAG.add_function(%Function{
          id: :b,
          args: [:a],
          code: fn a, multiplier -> a * multiplier end,
          extra_args: [10]
        })

      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               Executor.execute(dag_with_functions)

      assert returned_dag_id == dag.id
      assert actual_results[:a] == 1
      assert actual_results[:b] == 10
    end

    test "handles execution errors" do
      dag = Handout.DAG.new()

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :a,
          args: [],
          code: fn -> 1 end
        })
        |> DAG.add_function(%Function{
          id: :error,
          args: [:a],
          code: fn _ -> raise "An error occurred" end
        })

      {:error, error_info} = Executor.execute(dag_with_functions)
      assert match?({%RuntimeError{message: "An error occurred"}, _}, error_info)
    end

    test "rejects invalid DAGs" do
      # Create a cyclic DAG: A -> B -> A
      dag = Handout.DAG.new()

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :a,
          args: [:b],
          code: fn b -> b + 1 end
        })
        |> DAG.add_function(%Function{
          id: :b,
          args: [:a],
          code: fn a -> a * 2 end
        })

      assert {:error, {:cyclic_dependency, cycle}} = Executor.execute(dag_with_functions)
      assert Enum.sort(cycle) == [:a, :b]
    end

    @tag :skip
    test "can fetch a single element from an argument" do
      dag = Handout.DAG.new()

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
        |> Handout.execute()

      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} = execute_result
      assert returned_dag_id == dag.id
      assert actual_results == %{arg0: [1, 2, 3], arg1: [[10, 20, 30]], zip: [23, 33, 43]}
    end
  end
end
