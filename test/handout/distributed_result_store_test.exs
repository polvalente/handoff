defmodule Handout.DistributedResultStoreTest do
  use ExUnit.Case, async: false

  alias Handout.{
    ResultStore,
    DistributedResultStore,
    DataLocationRegistry,
    DistributedTestFunctions
  }

  @default_dag_id "test_dag"

  setup do
    # Get test nodes from application env
    assert [node2, node3] = Application.get_env(:handout, :test_nodes)

    # Clear stores for the default DAG ID
    ResultStore.clear(@default_dag_id)
    DataLocationRegistry.clear(@default_dag_id)
    # Also clear for other DAG IDs that might be used in specific tests
    DataLocationRegistry.clear("other_dag")
    ResultStore.clear("other_dag")

    {:ok, %{node2: node2, node3: node3, dag_id: @default_dag_id}}
  end

  describe "distributed result storage" do
    test "stores results locally and broadcasts to other nodes", %{dag_id: dag_id} do
      # In a single-node test, we're primarily testing the local functionality
      # Broadcasting would be tested in a multi-node environment

      # Store a result
      DistributedResultStore.store_distributed(dag_id, :function1, 42, Node.self())

      # Verify it's in the local result store
      assert {:ok, 42} = ResultStore.get(dag_id, :function1)
    end

    test "get_with_timeout returns result when available", %{dag_id: dag_id} do
      # Store a result
      ResultStore.store(dag_id, :function2, 100)

      # Should be available immediately
      assert {:ok, 100} = DistributedResultStore.get_with_timeout(dag_id, :function2, 1000)
    end

    test "get_with_timeout times out when result not available", %{dag_id: dag_id} do
      # This should timeout quickly
      assert {:error, :timeout} =
               DistributedResultStore.get_with_timeout(dag_id, :non_existent, 100)
    end

    test "get_with_timeout waits for result to be available", %{dag_id: dag_id} do
      # Start a task that will store a result after a delay
      Task.start(fn ->
        # Wait a bit
        :timer.sleep(200)
        ResultStore.store(dag_id, :delayed_function, 77)
      end)

      # Should wait and then return the result
      assert {:ok, 77} = DistributedResultStore.get_with_timeout(dag_id, :delayed_function, 1000)
    end

    test "clear_all_nodes clears local results for a dag", %{dag_id: dag_id} do
      other_dag_id = "other_dag"
      # For another DAG
      ResultStore.store(other_dag_id, :other_func, 999)

      # Store some results for current dag_id
      ResultStore.store(dag_id, :function3, 30)
      ResultStore.store(dag_id, :function4, 40)

      # Clear all nodes for current dag_id
      DistributedResultStore.clear_all_nodes(dag_id)

      # Verify results are gone for current dag_id
      assert {:error, :not_found} = ResultStore.get(dag_id, :function3)
      assert {:error, :not_found} = ResultStore.get(dag_id, :function4)

      # Verify result for other_dag_id still exists
      assert {:ok, 999} = ResultStore.get(other_dag_id, :other_func)
      # cleanup
      ResultStore.clear(other_dag_id)
    end

    test "synchronize fetches results from local node for a dag", %{dag_id: dag_id} do
      # Store some results
      ResultStore.store(dag_id, :function5, 50)
      ResultStore.store(dag_id, :function6, 60)

      # Register the locations
      DataLocationRegistry.register(dag_id, :function5, Node.self())
      DataLocationRegistry.register(dag_id, :function6, Node.self())

      # Clear local store and then synchronize
      # This test is a bit artificial for single node. `synchronize` primarily targets fetching from *remote* if not local.
      # To make it more meaningful, let's simulate the items are only in DataLocationRegistry initially.
      ResultStore.clear(dag_id)
      assert {:error, :not_found} = ResultStore.get(dag_id, :function5)

      # Simulate that another node (or this node before clear) had registered these.
      # For this test, it means synchronize will use get_with_fetch, which will find them via registry -> local (not present) -> remote (not present for real, but get_with_fetch might try if registry pointed elsewhere).
      # Given ResultStore.get_with_fetch will call ResultStore.get(dag_id, id) first, and we cleared, it won't find them immediately.
      # Then it calls fetch_remote which depends on DataLocationRegistry. If the items were just cleared, this test setup is tricky.
      # Let's re-store them to simulate they *are* available at the registered location (self() in this case).
      ResultStore.store(dag_id, :function5, 50)
      ResultStore.store(dag_id, :function6, 60)

      results =
        DistributedResultStore.synchronize(dag_id, [
          :function5,
          :function6,
          :function_non_existent
        ])

      assert results == %{function5: 50, function6: 60}
    end

    test "distributed fetching with location registry", %{node2: node2, dag_id: dag_id} do
      # 1. Create a result and register it on the remote node
      test_id = :dist_remote_test
      test_value = "distributed value"

      # Store the value on the remote node using the helper function
      result = :rpc.call(node2, DistributedTestFunctions, :f, [test_value])
      :ok = :rpc.call(node2, ResultStore, :store, [dag_id, test_id, result])

      # Register the location to point to the remote node
      :ok = DataLocationRegistry.register(dag_id, test_id, node2)

      # 2. Verify the value is not available locally
      assert {:error, :not_found} = ResultStore.get(dag_id, test_id)

      # 3. Test that get_with_timeout fetches from remote
      assert {:ok, ^result} = DistributedResultStore.get_with_timeout(dag_id, test_id, 1000)

      # 4. Verify that the value is now cached locally
      assert {:ok, ^result} = ResultStore.get(dag_id, test_id)
    end

    test "distributed fetching with multiple nodes", %{node2: node2, node3: node3, dag_id: dag_id} do
      # 1. Create different results on different nodes
      test_id1 = :dist_remote_test1
      test_id2 = :dist_remote_test2
      test_value1 = "value from node2"
      test_value2 = "value from node3"

      # Store values on different nodes using helper functions
      result_node2 = :rpc.call(node2, DistributedTestFunctions, :g, [test_id1, test_value1])

      result_node3 =
        :rpc.call(node3, DistributedTestFunctions, :h, [test_id2, test_value2, :extra])

      :ok = :rpc.call(node2, ResultStore, :store, [dag_id, test_id1, result_node2])
      :ok = :rpc.call(node3, ResultStore, :store, [dag_id, test_id2, result_node3])

      # Register the locations
      :ok = DataLocationRegistry.register(dag_id, test_id1, node2)
      :ok = DataLocationRegistry.register(dag_id, test_id2, node3)

      # 2. Verify values are not available locally
      assert {:error, :not_found} = ResultStore.get(dag_id, test_id1)
      assert {:error, :not_found} = ResultStore.get(dag_id, test_id2)

      # 3. Test that get_with_timeout fetches from both remotes
      assert {:ok, ^result_node2} =
               DistributedResultStore.get_with_timeout(dag_id, test_id1, 1000)

      assert {:ok, ^result_node3} =
               DistributedResultStore.get_with_timeout(dag_id, test_id2, 1000)

      # 4. Verify that both values are now cached locally
      assert {:ok, ^result_node2} = ResultStore.get(dag_id, test_id1)
      assert {:ok, ^result_node3} = ResultStore.get(dag_id, test_id2)
    end
  end

  # Note: In a real test environment, you would set up actual distributed Erlang nodes
  # and test the full distributed functionality. These tests only verify the basic functionality
  # in a single-node environment.
end
