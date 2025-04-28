defmodule Handout.DAGTest do
  use ExUnit.Case, async: true

  alias Handout.{DAG, Function}

  describe "new/0" do
    test "creates an empty DAG" do
      dag = DAG.new()
      assert dag.functions == %{}
      assert dag.dependencies == %{}
    end
  end

  describe "add_function/2" do
    test "adds a function to the DAG" do
      dag = DAG.new()

      function = %Function{
        id: :func1,
        args: [],
        code: fn -> :result1 end
      }

      updated_dag = DAG.add_function(dag, function)

      assert Map.has_key?(updated_dag.functions, :func1)
      assert updated_dag.functions[:func1] == function
    end

    test "updates dependencies when adding a function" do
      dag = DAG.new()

      # Add the first function that will be a dependency
      func1 = %Function{
        id: :func1,
        args: [],
        code: fn -> :result1 end
      }

      # Add a second function that depends on the first
      func2 = %Function{
        id: :func2,
        args: [:func1],
        code: fn results -> {:used, results[:func1]} end
      }

      dag =
        dag
        |> DAG.add_function(func1)
        |> DAG.add_function(func2)

      # func1 should have func2 as a dependent
      assert dag.dependencies[:func1] == [:func2]
    end
  end

  describe "validate/1" do
    test "validates a valid DAG" do
      dag = DAG.new()

      func1 = %Function{
        id: :func1,
        args: [],
        code: fn -> :result1 end
      }

      func2 = %Function{
        id: :func2,
        args: [:func1],
        code: fn results -> {:used, results[:func1]} end
      }

      dag =
        dag
        |> DAG.add_function(func1)
        |> DAG.add_function(func2)

      assert {:ok, _} = DAG.validate(dag)
    end

    test "detects missing dependencies" do
      dag = DAG.new()

      func = %Function{
        id: :func,
        args: [:missing_function],
        code: fn _ -> :result end
      }

      dag = DAG.add_function(dag, func)

      assert {:error, {:missing_dependencies, [:missing_function]}} = DAG.validate(dag)
    end

    test "detects cycles in the graph" do
      dag = DAG.new()

      func1 = %Function{
        id: :func1,
        args: [:func3],
        code: fn _ -> :result1 end
      }

      func2 = %Function{
        id: :func2,
        args: [:func1],
        code: fn _ -> :result2 end
      }

      func3 = %Function{
        id: :func3,
        args: [:func2],
        code: fn _ -> :result3 end
      }

      dag =
        dag
        |> DAG.add_function(func1)
        |> DAG.add_function(func2)
        |> DAG.add_function(func3)

      assert {:error, {:cycle_detected, _}} = DAG.validate(dag)
    end
  end
end
