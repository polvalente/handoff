defmodule Handout.ExecutorTest do
  use ExUnit.Case, async: false
  alias Handout.{Function, DAG, Executor, ResultStore}

  setup do
    start_supervised!(Handout.Supervisor)
    :ok
  end

  describe "single-node execution" do
    test "executes a simple DAG in dependency order" do
      # Create a simple DAG: A -> B -> C
      # Where A = 1, B = A + 1, C = B * 2

      dag =
        Handout.new()
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

      assert {:ok, results} = Executor.execute(dag)
      assert results[:a] == 1
      assert results[:b] == 2
      assert results[:c] == 4

      # Check results are in ResultStore
      assert {:ok, 1} = ResultStore.get(:a)
      assert {:ok, 2} = ResultStore.get(:b)
      assert {:ok, 4} = ResultStore.get(:c)
    end

    test "executes a DAG with multiple dependencies" do
      # Create a diamond DAG: A -> B -> D
      #                       \-> C -/

      dag =
        Handout.new()
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

      assert {:ok, results} = Executor.execute(dag)
      assert results[:a] == 5
      assert results[:b] == 7
      assert results[:c] == 10
      assert results[:d] == 17
    end

    test "handles a DAG with extra_args" do
      dag =
        Handout.new()
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

      assert {:ok, results} = Executor.execute(dag)
      assert results[:a] == 1
      assert results[:b] == 10
    end

    test "handles execution errors" do
      dag =
        Handout.new()
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

      {:error, error_info} = Executor.execute(dag)
      assert match?({%RuntimeError{message: "An error occurred"}, _}, error_info)
    end

    test "rejects invalid DAGs" do
      # Create a cyclic DAG: A -> B -> A
      dag =
        Handout.new()
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

      assert {:error, {:cycle_detected, _}} = Executor.execute(dag)
    end
  end
end
