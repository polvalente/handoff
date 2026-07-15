defmodule Handoff.DAGTest do
  use ExUnit.Case, async: false

  alias Handoff.DAG
  alias Handoff.Function
  alias Handoff.Function.Argument

  doctest Handoff.DAG

  describe "new/0" do
    test "creates an empty DAG" do
      dag = DAG.new()
      assert dag.functions == %{}
    end
  end

  describe "add_function/2" do
    test "adds a function to the DAG" do
      dag = DAG.new()

      function = %Function{
        id: :func1,
        args: [],
        code: &Elixir.Function.identity/1,
        extra_args: [:result1]
      }

      updated_dag = DAG.add_function(dag, function)

      assert Map.has_key?(updated_dag.functions, :func1)
      assert updated_dag.functions[:func1] == function
    end

    test "defaults streaming fields on Function struct" do
      function = %Function{
        id: :func1,
        args: [],
        code: &Elixir.Function.identity/1
      }

      assert function.init == nil
      assert function.parallelism == 1
      assert function.batch_size == nil
      assert function.batch_timeout == nil
    end

    test "accepts named-capture and MFA :init" do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{
          id: :with_capture_init,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [1],
          init: &Elixir.Function.identity/1
        })
        |> DAG.add_function(%Function{
          id: :with_mfa_init,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [2],
          init: {Elixir.Function, :identity, []}
        })

      assert Map.has_key?(dag.functions, :with_capture_init)
      assert Map.has_key?(dag.functions, :with_mfa_init)
    end

    test "rejects anonymous-function :init" do
      assert_raise RuntimeError, ~r/:init must be/, fn ->
        DAG.add_function(DAG.new(), %Function{
          id: :bad_init,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [1],
          init: fn -> :ok end
        })
      end
    end

    test "accepts type: :input with nil code" do
      function = %Function{
        id: :source,
        args: [],
        code: nil,
        type: :input
      }

      dag = DAG.add_function(DAG.new(), function)

      assert dag.functions[:source] == function
      assert :ok == DAG.validate(dag)
    end

    test "rejects :inline with :init" do
      dag =
        DAG.add_function(DAG.new(), %Function{
          id: :inline_with_init,
          args: [],
          type: :inline,
          code: &Elixir.Function.identity/1,
          extra_args: [1],
          init: {Elixir.Function, :identity, []}
        })

      assert {:error, {:invalid_inline_function_init, :inline_with_init}} = DAG.validate(dag)
    end

    test "rejects :inline with :node" do
      dag =
        DAG.add_function(DAG.new(), %Function{
          id: :inline_with_node,
          args: [],
          type: :inline,
          node: Node.self(),
          code: &Elixir.Function.identity/1,
          extra_args: [1]
        })

      assert {:error, {:invalid_inline_function_node, :inline_with_node}} = DAG.validate(dag)
    end
  end

  describe "validate/1" do
    test "validates a valid DAG" do
      dag = DAG.new()

      func1 = %Function{
        id: :func1,
        args: [],
        code: &Elixir.Function.identity/1,
        extra_args: [:result1]
      }

      func2 = %Function{
        id: :func2,
        args: [:func1],
        code: &Handoff.DistributedTestFunctions.g/2,
        extra_args: [:used]
      }

      dag =
        dag
        |> DAG.add_function(func1)
        |> DAG.add_function(func2)

      assert :ok == DAG.validate(dag)
    end

    test "detects missing dependencies" do
      dag = DAG.new()

      func = %Function{
        id: :func,
        args: [:some_function],
        code: &Elixir.Function.identity/1,
        extra_args: [:result]
      }

      dag = DAG.add_function(dag, func)

      assert {:error, {:missing_function, :some_function}} = DAG.validate(dag)
    end

    test "detects cycles in the graph" do
      dag = DAG.new()

      func1 = %Function{
        id: :func1,
        args: [:func3],
        code: &Elixir.Function.identity/1,
        extra_args: [:result1]
      }

      func2 = %Function{
        id: :func2,
        args: [:func1],
        code: &Handoff.DistributedTestFunctions.g/2,
        extra_args: [:result2]
      }

      func3 = %Function{
        id: :func3,
        args: [:func2],
        code: &Elixir.Function.identity/1,
        extra_args: [:result3]
      }

      dag =
        dag
        |> DAG.add_function(func1)
        |> DAG.add_function(func2)
        |> DAG.add_function(func3)

      assert {:error, {:cyclic_dependency, cycle}} = DAG.validate(dag)
      assert Enum.sort(cycle) == [:func2, :func3]
    end

    test "rejects type: :input with non-empty args" do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{
          id: :dep,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [1]
        })
        |> DAG.add_function(%Function{
          id: :source,
          args: [:dep],
          code: nil,
          type: :input
        })

      assert {:error, {:invalid_input_function, :source}} = DAG.validate(dag)
    end

    test "rejects type: :input with non-nil cost, code, or init" do
      for attrs <- [
            [cost: %{cpu: 1}],
            [code: &Elixir.Function.identity/1],
            [init: {Elixir.Function, :identity, []}]
          ] do
        function =
          struct!(
            %Function{id: :source, args: [], code: nil, type: :input},
            attrs
          )

        dag = DAG.add_function(DAG.new(), function)
        assert {:error, {:invalid_input_function, :source}} = DAG.validate(dag)
      end
    end
  end

  describe "add_function/2 with Handoff.Function.Argument" do
    test "expands Handoff.Function.Argument into synthetic nodes" do
      dag = Handoff.DAG.new()

      producer = %Handoff.Function{
        id: :producer,
        args: [],
        code: &Elixir.Function.identity/1,
        extra_args: [1]
      }

      consumer = %Handoff.Function{
        id: :consumer,
        args: [
          %Argument{
            id: :producer,
            serialization_fn: {Handoff.InternalOps, :identity_with_nodes, []},
            deserialization_fn: {Handoff.InternalOps, :identity_with_nodes, []}
          }
        ],
        code: &Elixir.Function.identity/1
      }

      dag = Handoff.DAG.add_function(dag, producer)
      dag = Handoff.DAG.add_function(dag, consumer)

      assert Enum.any?(
               dag.functions,
               fn {id, _} ->
                 match?(
                   {:serialize, _, :producer, :consumer,
                    {Handoff.InternalOps, :identity_with_nodes, []}},
                   id
                 )
               end
             )

      assert Enum.any?(
               dag.functions,
               fn {id, _} ->
                 match?(
                   {:deserialize, _, :producer, :consumer,
                    {Handoff.InternalOps, :identity_with_nodes, []}},
                   id
                 )
               end
             )
    end

    test "handles colocation directives" do
      dag = Handoff.DAG.new()

      producer = %Handoff.Function{
        id: :producer,
        args: [],
        code: &Elixir.Function.identity/1,
        extra_args: [1]
      }

      consumer = %Handoff.Function{
        id: :consumer,
        args: [
          %Argument{
            id: :producer,
            serialization_fn: {Handoff.InternalOps, :identity_with_nodes, []},
            deserialization_fn: {Handoff.InternalOps, :identity_with_nodes, []}
          }
        ],
        code: &Elixir.Function.identity/1,
        node: {:colocate_with_input, 0}
      }

      dag = Handoff.DAG.add_function(dag, producer)
      dag = Handoff.DAG.add_function(dag, consumer)

      assert Enum.any?(
               dag.functions,
               fn {id, _} ->
                 match?(
                   {:serialize, _, :producer, :consumer,
                    {Handoff.InternalOps, :identity_with_nodes, []}},
                   id
                 )
               end
             )

      assert Enum.any?(
               dag.functions,
               fn {id, _} ->
                 match?(
                   {:deserialize, _, :producer, :consumer,
                    {Handoff.InternalOps, :identity_with_nodes, []}},
                   id
                 )
               end
             )
    end
  end
end
