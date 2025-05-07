defmodule Handout.DistributedExecutorTest do
  use ExUnit.Case, async: false

  alias Handout.{DAG, Function, DistributedExecutor, SimpleResourceTracker}

  # Set up distributed test nodes
  # Note: In a real test environment, you would set up actual distributed Erlang nodes
  # For unit tests, we'll simulate distribution with local processes

  setup do
    # Start the application supervisors
    # Register local node with some capabilities
    SimpleResourceTracker.register(Node.self(), %{cpu: 4, memory: 2000})

    :ok
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
      # Define a simple DAG with two functions
      dag =
        DAG.new()
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
      assert {:ok, results} = DistributedExecutor.execute(dag)

      # Check results
      assert Map.get(results, :source) == 42
      assert Map.get(results, :squared) == 1764
    end

    test "can execute DAG with failure and retry" do
      # Define a more complex DAG with a function that fails once but succeeds on retry
      # We'll use an agent to track execution attempts
      {:ok, agent} = Agent.start_link(fn -> %{count: 0} end)

      dag =
        DAG.new()
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
      assert {:ok, results} = DistributedExecutor.execute(dag, opts)

      # Check results
      assert Map.get(results, :source) == 10
      assert Map.get(results, :fails_once) == 20
      assert Map.get(results, :final) == 25

      # Check that the function was called twice
      assert Agent.get(agent, fn state -> state.count end) == 2
    end
  end

  describe "resource management" do
    test "respects resource limits" do
      # Define functions that require more resources than available
      dag =
        DAG.new()
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
      assert {:error, _} = DistributedExecutor.execute(dag)

      # But a DAG with only the small resource function should succeed
      small_dag =
        DAG.new()
        |> DAG.add_function(%Function{
          id: :small_resource,
          args: [],
          code: fn -> 42 end,
          cost: %{cpu: 2, memory: 1000}
        })

      assert {:ok, results} = DistributedExecutor.execute(small_dag)
      assert Map.get(results, :small_resource) == 42
    end
  end
end
