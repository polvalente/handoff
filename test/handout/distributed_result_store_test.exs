defmodule Handout.DistributedResultStoreTest do
  use ExUnit.Case, async: false

  alias Handout.{
    ResultStore,
    DistributedResultStore,
    DataLocationRegistry,
    DistributedTestFunctions
  }

  setup do
    # Get test nodes from application env
    assert [node2, node3] = Application.get_env(:handout, :test_nodes)

    {:ok, %{node2: node2, node3: node3}}
  end

  describe "distributed result storage" do
    test "stores results locally and broadcasts to other nodes" do
      # In a single-node test, we're primarily testing the local functionality
      # Broadcasting would be tested in a multi-node environment

      # Store a result
      DistributedResultStore.store_distributed(:function1, 42)

      # Verify it's in the local result store
      assert {:ok, 42} = ResultStore.get(:function1)
    end

    test "get_with_timeout returns result when available" do
      # Store a result
      ResultStore.store(:function2, 100)

      # Should be available immediately
      assert {:ok, 100} = DistributedResultStore.get_with_timeout(:function2, 1000)
    end

    test "get_with_timeout times out when result not available" do
      # This should timeout quickly
      assert {:error, :timeout} = DistributedResultStore.get_with_timeout(:non_existent, 100)
    end

    test "get_with_timeout waits for result to be available" do
      # Start a task that will store a result after a delay
      Task.start(fn ->
        # Wait a bit
        :timer.sleep(200)
        ResultStore.store(:delayed_function, 77)
      end)

      # Should wait and then return the result
      assert {:ok, 77} = DistributedResultStore.get_with_timeout(:delayed_function, 1000)
    end

    test "clear_all_nodes clears local results" do
      # Store some results
      ResultStore.store(:function3, 30)
      ResultStore.store(:function4, 40)

      # Clear all nodes (in single-node test, this just clears local)
      DistributedResultStore.clear_all_nodes()

      # Verify results are gone
      assert {:error, :not_found} = ResultStore.get(:function3)
      assert {:error, :not_found} = ResultStore.get(:function4)
    end

    test "synchronize fetches results from local node" do
      # Store some results
      ResultStore.store(:function5, 50)
      ResultStore.store(:function6, 60)

      # Register the locations
      DataLocationRegistry.register(:function5, Node.self())
      DataLocationRegistry.register(:function6, Node.self())

      # Clear and then synchronize
      ResultStore.clear()

      # Verify results are gone
      assert {:error, :not_found} = ResultStore.get(:function5)

      # This would normally synchronize from other nodes
      # In a single-node test, we can't fully test this, but we can verify the API works
      results = DistributedResultStore.synchronize([:function5, :function6])

      # The synchronize in a single-node environment won't find anything since we cleared
      assert map_size(results) == 0
    end

    test "distributed fetching with location registry", %{node2: node2} do
      # 1. Create a result and register it on the remote node
      test_id = :dist_remote_test
      test_value = "distributed value"

      # Store the value on the remote node using the helper function
      result = :rpc.call(node2, DistributedTestFunctions, :f, [test_value])
      :ok = :rpc.call(node2, ResultStore, :store, [test_id, result])

      # Register the location to point to the remote node
      :ok = DataLocationRegistry.register(test_id, node2)

      # 2. Verify the value is not available locally
      assert {:error, :not_found} = ResultStore.get(test_id)

      # 3. Test that get_with_timeout fetches from remote
      assert {:ok, ^result} = DistributedResultStore.get_with_timeout(test_id, 1000)

      # 4. Verify that the value is now cached locally
      assert {:ok, ^result} = ResultStore.get(test_id)
    end

    test "distributed fetching with multiple nodes", %{node2: node2, node3: node3} do
      # 1. Create different results on different nodes
      test_id1 = :dist_remote_test1
      test_id2 = :dist_remote_test2
      test_value1 = "value from node2"
      test_value2 = "value from node3"

      # Store values on different nodes using helper functions
      result2 = :rpc.call(node2, DistributedTestFunctions, :g, [test_id1, test_value1])
      result3 = :rpc.call(node3, DistributedTestFunctions, :h, [test_id2, test_value2, :extra])

      :ok = :rpc.call(node2, ResultStore, :store, [test_id1, result2])
      :ok = :rpc.call(node3, ResultStore, :store, [test_id2, result3])

      # Register the locations
      :ok = DataLocationRegistry.register(test_id1, node2)
      :ok = DataLocationRegistry.register(test_id2, node3)

      # 2. Verify values are not available locally
      assert {:error, :not_found} = ResultStore.get(test_id1)
      assert {:error, :not_found} = ResultStore.get(test_id2)

      # 3. Test that get_with_timeout fetches from both remotes
      assert {:ok, ^result2} = DistributedResultStore.get_with_timeout(test_id1, 1000)
      assert {:ok, ^result3} = DistributedResultStore.get_with_timeout(test_id2, 1000)

      # 4. Verify that both values are now cached locally
      assert {:ok, ^result2} = ResultStore.get(test_id1)
      assert {:ok, ^result3} = ResultStore.get(test_id2)
    end
  end

  # Note: In a real test environment, you would set up actual distributed Erlang nodes
  # and test the full distributed functionality. These tests only verify the basic functionality
  # in a single-node environment.
end
