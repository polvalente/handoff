defmodule Handoff.DistributedExecutorTest do
  use ExUnit.Case, async: false

  alias Handoff.DAG
  alias Handoff.DistributedExecutor
  alias Handoff.Function
  alias Handoff.Function.Argument
  alias Handoff.ResultStore
  alias Handoff.SimpleResourceTracker

  setup do
    # Register local node with some capabilities
    [node_2 | _] = Application.get_env(:handoff, :test_nodes)
    SimpleResourceTracker.register(Node.self(), %{cpu: 4, memory: 2000})
    :rpc.call(node_2, SimpleResourceTracker, :register, [node_2, %{cpu: 4, memory: 2000}])
    %{node_2: node_2}
  end

  describe "node discovery" do
    test "can discover local node capabilities" do
      assert {:ok, discovered} = DistributedExecutor.discover_nodes()
      assert Map.has_key?(discovered, Node.self())
      assert %{cpu: 4, memory: 2000} = Map.get(discovered, Node.self())
    end
  end

  describe "distributed execution" do
    test "can execute simple DAG on local node" do
      dag = DAG.new(self())

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :source,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [42],
          cost: %{cpu: 1, memory: 100}
        })
        |> DAG.add_function(%Function{
          id: :squared,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.g/2,
          extra_args: [2],
          cost: %{cpu: 1, memory: 100}
        })

      # Execute the DAG
      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               DistributedExecutor.execute(dag_with_functions)

      assert returned_dag_id == dag.id

      # Check results
      assert Map.get(actual_results, :source) == 42
      assert Map.get(actual_results, :squared) == [42, 2]
    end

    test "can execute simple DAG on two nodes", %{node_2: node_2} do
      dag = DAG.new(self())

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :source,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [42],
          node: Node.self(),
          cost: %{cpu: 1, memory: 1950}
        })
        |> DAG.add_function(%Function{
          id: :concatenated,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.g/2,
          extra_args: [1337],
          cost: %{cpu: 1, memory: 2000}
        })
        |> DAG.add_function(%Function{
          id: :final,
          args: [:concatenated],
          code: &Handoff.DistributedTestFunctions.f/1,
          extra_args: [],
          cost: %{cpu: 1, memory: 50}
        })

      # Execute the DAG
      assert {:ok,
              %{
                dag_id: returned_dag_id,
                results: actual_results,
                allocations: allocations
              }} =
               DistributedExecutor.execute(dag_with_functions)

      assert allocations == %{source: Node.self(), concatenated: node_2, final: Node.self()}

      assert returned_dag_id == dag.id

      # Check results
      assert Map.get(actual_results, :source) == 42

      # For functions executed remotely,
      # the result is registered but not included directly in results
      assert Map.get(actual_results, :concatenated) == :remote_executed_and_registered

      # We need to fetch the remote result directly
      {:ok, concatenated_result} =
        :rpc.call(node_2, Handoff.ResultStore, :get, [dag.id, :concatenated])

      assert concatenated_result == [42, 1337]

      # The final function uses the result it fetched from the remote node
      assert Map.get(actual_results, :final) == [[42, 1337]]

      # Double-check result is accessible on the remote node
      assert {:ok, [42, 1337]} =
               :rpc.call(node_2, Handoff.ResultStore, :get, [dag.id, :concatenated])
    end

    test "can execute simple DAG on two nodes with another function forced to self", %{
      node_2: node_2
    } do
      dag = DAG.new(self())

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :source,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [42],
          cost: %{cpu: 1, memory: 1950}
        })
        |> DAG.add_function(%Function{
          id: :concatenated,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.g/2,
          extra_args: [1337],
          node: Node.self(),
          cost: %{cpu: 1, memory: 100}
        })
        |> DAG.add_function(%Function{
          id: :final,
          args: [:concatenated],
          code: &Handoff.DistributedTestFunctions.f/1,
          extra_args: [],
          node: node_2,
          cost: %{cpu: 1, memory: 50}
        })

      # Execute the DAG
      assert {:ok,
              %{
                dag_id: returned_dag_id,
                results: actual_results,
                allocations: allocations
              }} =
               DistributedExecutor.execute(dag_with_functions)

      assert allocations == %{source: node_2, concatenated: Node.self(), final: node_2}

      assert returned_dag_id == dag.id

      # Check results
      assert Map.get(actual_results, :source) == :remote_executed_and_registered

      # We need to fetch the remote result directly
      {:ok, source_result} =
        :rpc.call(node_2, Handoff.ResultStore, :get, [dag.id, :source])

      assert source_result == 42

      # For functions executed remotely,
      # the result is registered but not included directly in results
      assert Map.get(actual_results, :concatenated) == [42, 1337]

      # The final function uses the result it fetched from the remote node
      assert Map.get(actual_results, :final) == :remote_executed_and_registered

      # Double-check result is accessible on the remote node
      assert {:ok, [42, 1337]} =
               :rpc.call(node_2, Handoff.ResultStore, :get, [dag.id, :concatenated])
    end

    test "can execute DAG with failure and retry" do
      # Use specific DAG ID
      dag = DAG.new(self())
      # We'll use an agent to track execution attempts
      {:ok, agent} = Agent.start_link(fn -> %{count: 0} end)

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :source,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [10],
          cost: %{cpu: 1, memory: 100}
        })
        |> DAG.add_function(%Function{
          id: :fails_once,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.failing_function/2,
          extra_args: [agent]
        })
        |> DAG.add_function(%Function{
          id: :final,
          args: [:fails_once],
          code: &Handoff.DistributedTestFunctions.g/2,
          extra_args: [5]
        })

      # Set max retries to 1 to ensure it retries once
      opts = [max_retries: 1]

      # Execute the DAG
      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               DistributedExecutor.execute(dag_with_functions, opts)

      assert returned_dag_id == dag.id

      # Check results
      assert Map.get(actual_results, :source) == 10
      assert Map.get(actual_results, :fails_once) == 20
      assert Map.get(actual_results, :final) == [20, 5]

      # Check that the function was called twice
      assert Agent.get(agent, fn state -> state.count end) == 2
    end

    test "can choose to pass arguments as a list instead of requiring variadic functions" do
      # Use specific DAG ID
      dag = DAG.new(self())

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :a,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [10]
        })
        |> DAG.add_function(%Function{
          id: :b,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [20]
        })
        |> DAG.add_function(%Function{
          id: :sum,
          args: [:a, :b],
          code: &Enum.sum/1,
          extra_args: [],
          argument_inclusion: :as_list
        })

      # Execute the DAG
      assert {:ok, %{results: actual_results}} =
               DistributedExecutor.execute(dag_with_functions)

      # Check results
      assert Map.get(actual_results, :a) == 10
      assert Map.get(actual_results, :b) == 20
      assert Map.get(actual_results, :sum) == 30
    end
  end

  describe "inline function execution" do
    test "executes inline function multiple times when depended upon by multiple functions" do
      dag = DAG.new({self(), :inline_test})
      {:ok, counter_agent} = Agent.start_link(fn -> %{count: 0} end)

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          # Inline function
          id: :a,
          args: [],
          type: :inline,
          # As per validation rule for inline
          node: nil,
          code: &Handoff.DistributedTestFunctions.counting_identity_function/2,
          extra_args: [10, counter_agent]
        })
        |> DAG.add_function(%Function{
          # Depends on :a
          id: :b,
          args: [:a],
          code: &Kernel.+/2,
          # b_result = a_result + 5
          extra_args: [5],
          # Regular function, assign cost
          cost: %{cpu: 1, memory: 100}
        })
        |> DAG.add_function(%Function{
          # Depends on :a
          id: :c,
          args: [:a],
          code: &*/2,
          # c_result = a_result * 3
          extra_args: [3],
          cost: %{cpu: 1, memory: 100}
        })
        |> DAG.add_function(%Function{
          id: :d,
          args: [:b, :c],
          code: &Handoff.DistributedTestFunctions.g/2,
          cost: %{cpu: 1, memory: 100}
        })

      # Execute the DAG (all on local node for simplicity of testing inline behavior)
      assert {:ok, %{results: actual_results}} =
               DistributedExecutor.execute(dag_with_functions)

      # Check results
      # :a is inline, so it won't be in final results map
      assert not Map.has_key?(actual_results, :a)
      # :a should return 10. So :b = 10 + 5 = 15
      assert Map.get(actual_results, :b) == 15
      # :a should return 10. So :c = 10 * 3 = 30
      assert Map.get(actual_results, :c) == 30
      # :d depends on :b and :c
      assert Map.get(actual_results, :d) == [15, 30]

      # Check that :a was called twice (once for :b, once for :c)
      assert Agent.get(counter_agent, fn state -> state.count end) == 2
    end
  end

  test "fails when resource constraints not satisfied" do
    dag = DAG.new(self())

    for {node_a, node_b, task_that_fails} <- [
          {Node.self(), Node.self(), :task_B},
          {Node.self(), {:collocated, :task_A}, :task_A}
        ] do
      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :task_A,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: ["A"],
          node: node_a,
          # Consumes most of self_node's memory if allocated there
          cost: %{cpu: 1, memory: 1900}
        })
        |> DAG.add_function(%Function{
          id: :task_B,
          # Independent task
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: ["B"],
          node: node_b,
          # Requires more memory than self_node would have left
          cost: %{cpu: 1, memory: 150}
        })

      # Explicitly use the :first_available strategy for clarity
      opts = [allocation_strategy: :first_available]

      assert {:error,
              {:allocation_error,
               "Insufficient resources on node #{inspect(Node.self())} for function #{inspect(task_that_fails)}"}} ==
               DistributedExecutor.execute(dag_with_functions, opts)
    end
  end

  describe "resource management" do
    test "respects resource limits" do
      dag1_id = {self(), 1}
      dag2_id = {self(), 2}

      # Define functions that require more resources than available
      dag_fail =
        dag1_id
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :small_resource,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [42],
          cost: %{cpu: 2, memory: 1000}
        })
        |> DAG.add_function(%Function{
          id: :large_resource,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [100],
          # Exceeds available resources
          cost: %{cpu: 10, memory: 1500}
        })
        |> DAG.add_function(%Function{
          id: :dependent,
          args: [:small_resource, :large_resource],
          code: &Handoff.DistributedTestFunctions.g/2,
          extra_args: [5]
        })

      # This execution should fail because of resource constraints
      assert {:error, _} = DistributedExecutor.execute(dag_fail)

      # But a DAG with only the small resource function should succeed
      small_dag =
        dag2_id
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :small_resource,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [42],
          cost: %{cpu: 2, memory: 1000}
        })

      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               DistributedExecutor.execute(small_dag)

      assert returned_dag_id == small_dag.id
      assert Map.get(actual_results, :small_resource) == 42
    end

    test "merges collocated costs" do
      [node_2 | _] = Application.get_env(:handoff, :test_nodes)
      SimpleResourceTracker.register(Node.self(), %{cpu: 4})
      :rpc.call(node_2, SimpleResourceTracker, :register, [node_2, %{cpu: 0}])

      dag =
        Enum.reduce(1..4, DAG.new(), fn
          1, dag ->
            DAG.add_function(
              dag,
              %Function{
                id: :f1,
                code: &Elixir.Function.identity/1,
                args: [],
                extra_args: [42],
                cost: %{cpu: 1},
                node: Node.self()
              }
            )

          i, dag ->
            DAG.add_function(
              dag,
              %Function{
                id: :"f#{i}",
                code: &Elixir.Function.identity/1,
                args: [:"f#{i - 1}"],
                extra_args: [],
                cost: %{cpu: 1},
                node: {:collocated, :"f#{i - 1}"}
              }
            )
        end)
        |> DAG.add_function(%Function{
          id: :f5,
          code: &Elixir.Function.identity/1,
          args: [],
          extra_args: [1337],
          cost: %{cpu: 0}
        })

      assert {:ok, %{allocations: allocations}} = DistributedExecutor.execute(dag)
      assert Enum.all?(allocations, fn {_, value} -> value == Node.self() end)

      dag =
        Enum.reduce(1..4, DAG.new(), fn
          1, dag ->
            DAG.add_function(
              dag,
              %Function{
                id: :f1,
                code: &Elixir.Function.identity/1,
                args: [],
                extra_args: [42],
                cost: %{cpu: 1},
                node: node_2
              }
            )

          i, dag ->
            DAG.add_function(
              dag,
              %Function{
                id: :"f#{i}",
                code: &Elixir.Function.identity/1,
                args: [:"f#{i - 1}"],
                extra_args: [],
                cost: %{cpu: 1},
                node: {:collocated, :f1}
              }
            )
        end)

      assert {:error,
              {:allocation_error,
               "Insufficient resources on node #{inspect(node_2)} for function :f1"}} ==
               DistributedExecutor.execute(dag)
    end
  end

  describe "execute_function_on_node with synthetic nodes" do
    test "injects source_node and target_node for serializer" do
      dag = Handoff.DAG.new()

      producer = %Handoff.Function{
        id: :producer,
        args: [],
        code: &Elixir.Function.identity/1,
        extra_args: [[1, 2]]
      }

      consumer = %Handoff.Function{
        id: :consumer,
        args: [
          %Argument{
            id: :producer,
            serialization_fn: {Handoff.DistributedTestFunctions, :serialize, []},
            deserialization_fn: {Handoff.DistributedTestFunctions, :deserialize, []}
          }
        ],
        code: &Elixir.Function.identity/1
      }

      dag = Handoff.DAG.add_function(dag, producer)
      dag = Handoff.DAG.add_function(dag, consumer)

      assert {:ok, %{results: results}} = Handoff.DistributedExecutor.execute(dag)

      # same node serde should not be serialized as per the function's implementation
      {_id, value} =
        Enum.find(results, fn {id, _} ->
          match?({:serialize, _, :producer, :consumer, _}, id)
        end)

      assert value == [1, 2]

      {_id, value} =
        Enum.find(results, fn {id, _} ->
          match?({:deserialize, _, :producer, :consumer, _}, id)
        end)

      assert value == [1, 2]

      assert results[:producer] == [1, 2]
      assert results[:consumer] == [1, 2]
    end

    test "serializer can be used to fetch a single entry of a returning tuple", %{node_2: node_2} do
      dag = Handoff.DAG.new()

      producer = %Handoff.Function{
        id: :producer,
        args: [],
        code: &Elixir.Function.identity/1,
        extra_args: [{[1, 2], [3, 4]}],
        node: Node.self()
      }

      consumer = %Handoff.Function{
        id: :consumer,
        args: [
          %Argument{
            id: :producer,
            serialization_fn: {Handoff.DistributedTestFunctions, :elem_with_nodes, [1]},
            deserialization_fn: {Handoff.InternalOps, :identity_with_nodes, []}
          }
        ],
        code: &Elixir.Function.identity/1,
        node: node_2
      }

      dag = Handoff.DAG.add_function(dag, producer)
      dag = Handoff.DAG.add_function(dag, consumer)

      assert {:ok, %{results: results}} = Handoff.DistributedExecutor.execute(dag)

      # same node serde should not be serialized as per the function's implementation
      {_id, value} =
        Enum.find(results, fn {id, _} ->
          match?({:serialize, _, :producer, :consumer, _}, id)
        end)

      assert value == [3, 4]

      {id, _} =
        Enum.find(results, fn {id, _} ->
          match?({:deserialize, _, :producer, :consumer, _}, id)
        end)

      {:ok, deserialized_result} =
        :rpc.call(node_2, Handoff.ResultStore, :get, [dag.id, id])

      assert deserialized_result == [3, 4]

      assert results[:producer] == {[1, 2], [3, 4]}

      {:ok, consumer_result} = :rpc.call(node_2, Handoff.ResultStore, :get, [dag.id, :consumer])
      assert consumer_result == [3, 4]
    end

    test "injects source_node and target_node for deserializer" do
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

      assert {:ok, %{results: results}} = Handoff.DistributedExecutor.execute(dag)
      assert results[:producer] == 1
      assert results[:consumer] == 1
    end
  end

  test "serializes when nodes are different", %{node_2: node_2} do
    dag = Handoff.DAG.new()

    producer = %Handoff.Function{
      id: :producer,
      args: [],
      code: &Elixir.Function.identity/1,
      extra_args: [[1, 2]],
      node: Node.self()
    }

    consumer = %Handoff.Function{
      id: :consumer,
      args: [
        %Argument{
          id: :producer,
          serialization_fn: {Handoff.DistributedTestFunctions, :serialize, []},
          deserialization_fn: {Handoff.DistributedTestFunctions, :deserialize, []}
        }
      ],
      code: &Elixir.Function.identity/1,
      node: node_2
    }

    dag = Handoff.DAG.add_function(dag, producer)
    dag = Handoff.DAG.add_function(dag, consumer)

    assert {:ok, %{results: results, allocations: allocations}} =
             Handoff.DistributedExecutor.execute(dag)

    self_node = Node.self()

    assert %{
             producer: ^self_node,
             consumer: ^node_2
           } = allocations

    assert {serialize_id, ^self_node} =
             Enum.find(allocations, fn {id, _} ->
               match?(
                 {:serialize, _, :producer, :consumer,
                  {Handoff.DistributedTestFunctions, :serialize, []}},
                 id
               )
             end)

    assert {deserialize_id, ^node_2} =
             Enum.find(allocations, fn {id, _} ->
               match?(
                 {:deserialize, _, :producer, :consumer,
                  {Handoff.DistributedTestFunctions, :deserialize, []}},
                 id
               )
             end)

    assert results[:producer] == [1, 2]

    assert results[serialize_id] == :erlang.term_to_binary([1, 2])

    {:ok, deserialized_result} =
      :rpc.call(node_2, Handoff.ResultStore, :get, [dag.id, deserialize_id])

    assert deserialized_result == [1, 2]

    assert Map.get(results, :consumer) == :remote_executed_and_registered

    # We need to fetch the remote result directly
    {:ok, consumer_result} =
      :rpc.call(node_2, Handoff.ResultStore, :get, [dag.id, :consumer])

    assert consumer_result == [1, 2]
  end

  describe "local execution via DistributedExecutor" do
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
          code: &*/2,
          extra_args: [2]
        })

      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               DistributedExecutor.execute(dag_with_functions)

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
      #                       \\-> C -/

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
          code: &*/2,
          extra_args: [2]
        })
        |> DAG.add_function(%Function{
          id: :d,
          args: [:b, :c],
          code: &Kernel.+/2
        })

      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               DistributedExecutor.execute(dag_with_functions)

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
          code: &*/2,
          extra_args: [10]
        })

      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               DistributedExecutor.execute(dag_with_functions)

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

      {:ok, %{results: %{error: {:error, error_message}}}} =
        DistributedExecutor.execute(dag_with_functions)

      assert String.contains?(error_message, "An error occurred, value: 1")
    end

    test "rejects invalid DAGs (validation happens before execution)" do
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
          code: &*/2,
          extra_args: [2]
        })

      # DistributedExecutor.execute calls DAG.validate first
      assert {:error, {:cyclic_dependency, cycle}} =
               DistributedExecutor.execute(dag_with_functions)

      assert Enum.sort(cycle) == [:a, :b]
    end
  end
end
