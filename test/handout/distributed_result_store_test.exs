defmodule Handout.DistributedResultStoreTest do
  use ExUnit.Case, async: false

  alias Handout.{ResultStore, DistributedResultStore}

  setup do
    # Start the necessary processes
    start_supervised!(Handout.ResultStore)
    start_supervised!(Handout.DistributedResultStore)

    # Clear any results between tests
    ResultStore.clear()

    :ok
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
  end

  # Note: In a real test environment, you would set up actual distributed Erlang nodes
  # and test the full distributed functionality. These tests only verify the basic functionality
  # in a single-node environment.
end
