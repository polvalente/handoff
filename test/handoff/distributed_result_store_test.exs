defmodule Handoff.DistributedResultStoreTest do
  use ExUnit.Case, async: false

  alias Handoff.DataLocationRegistry
  alias Handoff.DistributedResultStore
  alias Handoff.ResultStore

  @dag_id_a "test_dag_a"
  @dag_id_b "test_dag_b"

  setup do
    # Get test nodes from application env
    assert [node2, node3] = Application.get_env(:handoff, :test_nodes)

    # Clear stores for the default DAG ID
    ResultStore.clear(@dag_id_a)
    ResultStore.clear(@dag_id_b)
    DataLocationRegistry.clear(@dag_id_a)
    DataLocationRegistry.clear(@dag_id_b)
    # Also clear for other DAG IDs that might be used in specific tests
    DataLocationRegistry.clear("other_dag")
    ResultStore.clear("other_dag")

    {:ok, %{node2: node2, node3: node3, dag_id_a: @dag_id_a, dag_id_b: @dag_id_b}}
  end

  describe "distributed result storage with DAG ID scoping" do
    test "store_distributed is scoped by DAG ID", %{dag_id_a: dag_id_a, dag_id_b: dag_id_b} do
      DistributedResultStore.store_distributed(dag_id_a, :item1, "value_a", Node.self())
      DistributedResultStore.store_distributed(dag_id_b, :item1, "value_b", Node.self())

      assert {:ok, "value_a"} = ResultStore.get(dag_id_a, :item1)
      assert {:ok, "value_b"} = ResultStore.get(dag_id_b, :item1)

      {:ok, node_a_lookup} = DataLocationRegistry.lookup(dag_id_a, :item1)
      assert node_a_lookup == Node.self()
      {:ok, node_b_lookup} = DataLocationRegistry.lookup(dag_id_b, :item1)
      assert node_b_lookup == Node.self()
    end

    test "broadcast_result is scoped by DAG ID (local check)", %{
      dag_id_a: dag_id_a,
      dag_id_b: dag_id_b,
      node2: _node2
    } do
      # This test primarily checks local ResultStore impact and
      # that the cast message would be scoped.
      # True broadcast verification requires checking remote nodes,
      # which is harder in unit tests without full cluster setup.
      DistributedResultStore.broadcast_result(dag_id_a, :item_broadcast, "val_a")
      # Simulate a broadcast for another DAG ID for a different item, or even same item
      DistributedResultStore.broadcast_result(dag_id_b, :item_broadcast_b, "val_b")

      assert {:ok, "val_a"} = ResultStore.get(dag_id_a, :item_broadcast)
      assert {:ok, "val_b"} = ResultStore.get(dag_id_b, :item_broadcast_b)

      # To-do: A more robust test would mock/spy on :rpc.cast to ensure it's called with correct dag_id.
    end

    test "get_with_timeout is scoped by DAG ID", %{dag_id_a: dag_id_a, dag_id_b: dag_id_b} do
      ResultStore.store(dag_id_a, :item_timeout, "value_a_timeout")
      ResultStore.store(dag_id_b, :item_timeout, "value_b_timeout")

      assert {:ok, "value_a_timeout"} =
               DistributedResultStore.get_with_timeout(dag_id_a, :item_timeout, 100)

      assert {:ok, "value_b_timeout"} =
               DistributedResultStore.get_with_timeout(dag_id_b, :item_timeout, 100)

      assert {:error, :timeout} =
               DistributedResultStore.get_with_timeout(dag_id_a, :non_existent, 10)
    end

    test "clear_all_nodes is scoped by DAG ID", %{dag_id_a: dag_id_a, dag_id_b: dag_id_b} do
      ResultStore.store(dag_id_a, :item_clear_a, "data_a")
      ResultStore.store(dag_id_b, :item_clear_b, "data_b")
      DataLocationRegistry.register(dag_id_a, :item_clear_a, Node.self())
      DataLocationRegistry.register(dag_id_b, :item_clear_b, Node.self())

      DistributedResultStore.clear_all_nodes(dag_id_a)

      assert {:error, :not_found} = ResultStore.get(dag_id_a, :item_clear_a)
      assert {:error, :not_found} = DataLocationRegistry.lookup(dag_id_a, :item_clear_a)

      assert {:ok, "data_b"} = ResultStore.get(dag_id_b, :item_clear_b)
      {:ok, node_b_clear_lookup} = DataLocationRegistry.lookup(dag_id_b, :item_clear_b)
      assert node_b_clear_lookup == Node.self()
    end

    test "synchronize is scoped by DAG ID", %{dag_id_a: dag_id_a, dag_id_b: dag_id_b} do
      ResultStore.store(dag_id_a, :sync_item_a, "val_sync_a")
      DataLocationRegistry.register(dag_id_a, :sync_item_a, Node.self())
      ResultStore.store(dag_id_b, :sync_item_b, "val_sync_b")
      DataLocationRegistry.register(dag_id_b, :sync_item_b, Node.self())

      # Clear local ResultStore for dag_id_a to force synchronize to (try to) fetch
      ResultStore.clear(dag_id_a)
      assert {:error, :not_found} = ResultStore.get(dag_id_a, :sync_item_a)
      # Put it back for fetch_remote to find
      ResultStore.store(dag_id_a, :sync_item_a, "val_sync_a")

      results_a = DistributedResultStore.synchronize(dag_id_a, [:sync_item_a, :non_existent_a])
      assert results_a == %{sync_item_a: "val_sync_a"}

      results_b = DistributedResultStore.synchronize(dag_id_b, [:sync_item_b, :non_existent_b])
      assert results_b == %{sync_item_b: "val_sync_b"}
    end

    test "distributed fetching with location registry is scoped by DAG ID", %{
      node2: node2,
      dag_id_a: dag_id_a,
      dag_id_b: dag_id_b
    } do
      item_id = :dist_fetch_scoped
      val_a = "value_for_a"
      val_b = "value_for_b"

      # Store for dag_id_a on node2
      :rpc.call(node2, ResultStore, :store, [dag_id_a, item_id, val_a])
      DataLocationRegistry.register(dag_id_a, item_id, node2)

      # Store for dag_id_b on node2 (same item_id, different dag_id)
      :rpc.call(node2, ResultStore, :store, [dag_id_b, item_id, val_b])
      DataLocationRegistry.register(dag_id_b, item_id, node2)

      # Verify not local for either DAG ID initially
      assert {:error, :not_found} = ResultStore.get(dag_id_a, item_id)
      assert {:error, :not_found} = ResultStore.get(dag_id_b, item_id)

      # Fetch for dag_id_a
      assert {:ok, ^val_a} = DistributedResultStore.get_with_timeout(dag_id_a, item_id, 5000)
      # Now cached locally for dag_id_a
      assert {:ok, ^val_a} = ResultStore.get(dag_id_a, item_id)
      # dag_id_b should still not be locally cached by this operation
      assert {:error, :not_found} = ResultStore.get(dag_id_b, item_id)

      # Fetch for dag_id_b
      assert {:ok, ^val_b} = DistributedResultStore.get_with_timeout(dag_id_b, item_id, 5000)
      # Now cached locally for dag_id_b
      assert {:ok, ^val_b} = ResultStore.get(dag_id_b, item_id)
    end

    # Original tests, adapted slightly if needed, can be kept or refactored further.
    # For brevity, focusing on adding new scoping tests first.
    # The following are pre-existing tests,
    # slightly adjusted to use one of the dag_ids for clarity.

    test "original: get_with_timeout returns result when available", %{dag_id_a: dag_id} do
      ResultStore.store(dag_id, :function2_orig, 100)
      assert {:ok, 100} = DistributedResultStore.get_with_timeout(dag_id, :function2_orig, 1000)
    end

    test "original: get_with_timeout times out when result not available", %{dag_id_a: dag_id} do
      assert {:error, :timeout} =
               DistributedResultStore.get_with_timeout(dag_id, :non_existent_orig, 100)
    end

    test "original: get_with_timeout waits for result to be available", %{dag_id_a: dag_id} do
      Task.start(fn ->
        :timer.sleep(200)
        ResultStore.store(dag_id, :delayed_function_orig, 77)
      end)

      assert {:ok, 77} =
               DistributedResultStore.get_with_timeout(dag_id, :delayed_function_orig, 1000)
    end
  end

  # Note: In a real test environment, you would set up actual distributed Erlang nodes
  # and test the full distributed functionality. These tests only verify the basic functionality
  # in a single-node environment.
end
