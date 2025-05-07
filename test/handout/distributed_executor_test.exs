defmodule Handout.DistributedExecutorTest do
  use ExUnit.Case, async: true

  alias Handout.{
    DAG,
    Function,
    DistributedExecutor,
    SimpleResourceTracker
  }

  setup do
    # Register local node with some capabilities
    [node_2 | _] = Application.get_env(:handout, :test_nodes)
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
          code: fn -> 42 end,
          cost: %{cpu: 1, memory: 100}
        })
        |> DAG.add_function(%Function{
          id: :squared,
          args: [:source],
          code: fn x -> x * x end,
          cost: %{cpu: 1, memory: 100}
        })

      # Execute the DAG
      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               DistributedExecutor.execute(dag_with_functions)

      assert returned_dag_id == dag.id

      # Check results
      assert Map.get(actual_results, :source) == 42
      assert Map.get(actual_results, :squared) == 1764
    end

    test "can execute simple DAG on two nodes", %{node_2: node_2} do
      dag = DAG.new(self())

      dag_with_functions =
        dag
        |> DAG.add_function(%Function{
          id: :source,
          args: [],
          code: fn -> 42 end,
          cost: %{cpu: 1, memory: 1950}
        })
        |> DAG.add_function(%Function{
          id: :concatenated,
          args: [:source],
          code: &Handout.DistributedTestFunctions.g/2,
          extra_args: [1337],
          cost: %{cpu: 1, memory: 100}
        })
        |> DAG.add_function(%Function{
          id: :final,
          args: [:concatenated],
          code: &Handout.DistributedTestFunctions.f/1,
          extra_args: [],
          cost: %{cpu: 1, memory: 50}
        })

      # Execute the DAG
      assert {:ok, %{dag_id: returned_dag_id, results: actual_results, allocations: allocations}} =
               DistributedExecutor.execute(dag_with_functions)

      assert allocations == %{source: Node.self(), concatenated: node_2, final: Node.self()}

      assert returned_dag_id == dag.id

      # Check results
      assert Map.get(actual_results, :source) == 42

      # For functions executed remotely, the result is registered but not included directly in results
      assert Map.get(actual_results, :concatenated) == :remote_executed_and_registered

      # We need to fetch the remote result directly
      {:ok, concatenated_result} =
        :rpc.call(node_2, Handout.ResultStore, :get, [dag.id, :concatenated])

      assert concatenated_result == [42, 1337]

      # The final function uses the result it fetched from the remote node
      assert Map.get(actual_results, :final) == [[42, 1337]]

      # Double-check result is accessible on the remote node
      assert {:ok, [42, 1337]} =
               :rpc.call(node_2, Handout.ResultStore, :get, [dag.id, :concatenated])
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
          code: fn -> 10 end
        })
        |> DAG.add_function(%Function{
          id: :fails_once,
          args: [:source],
          code: fn x ->
            count = Agent.get(agent, fn state -> state.count end)
            Agent.update(agent, fn state -> %{state | count: state.count + 1} end)

            if count == 0 do
              # First call fails
              raise "Simulated failure for testing"
            else
              # Second call succeeds
              x * 2
            end
          end
        })
        |> DAG.add_function(%Function{
          id: :final,
          args: [:fails_once],
          code: fn x -> x + 5 end
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
      assert Map.get(actual_results, :final) == 25

      # Check that the function was called twice
      assert Agent.get(agent, fn state -> state.count end) == 2
    end
  end

  describe "resource management" do
    test "respects resource limits" do
      dag1_id = {self(), 1}
      dag2_id = {self(), 2}

      # Define functions that require more resources than available
      dag_fail =
        DAG.new(dag1_id)
        |> DAG.add_function(%Function{
          id: :small_resource,
          args: [],
          code: fn -> 42 end,
          cost: %{cpu: 2, memory: 1000}
        })
        |> DAG.add_function(%Function{
          id: :large_resource,
          args: [],
          code: fn -> 100 end,
          # Exceeds available resources
          cost: %{cpu: 10, memory: 5000}
        })
        |> DAG.add_function(%Function{
          id: :dependent,
          args: [:small_resource, :large_resource],
          code: fn a, b -> a + b end
        })

      # This execution should fail because of resource constraints
      assert {:error, _} = DistributedExecutor.execute(dag_fail)

      # But a DAG with only the small resource function should succeed
      small_dag =
        DAG.new(dag2_id)
        |> DAG.add_function(%Function{
          id: :small_resource,
          args: [],
          code: fn -> 42 end,
          cost: %{cpu: 2, memory: 1000}
        })

      assert {:ok, %{dag_id: returned_dag_id, results: actual_results}} =
               DistributedExecutor.execute(small_dag)

      assert returned_dag_id == small_dag.id
      assert Map.get(actual_results, :small_resource) == 42
    end
  end
end
