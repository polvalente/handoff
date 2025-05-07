defmodule Handoff.DataLocationRegistryTest do
  use ExUnit.Case, async: false

  alias Handoff.DataLocationRegistry

  @dag_id_a "test_dag_a"
  @dag_id_b "test_dag_b"

  setup do
    # Clear the registry for the test DAG IDs between tests
    DataLocationRegistry.clear(@dag_id_a)
    DataLocationRegistry.clear(@dag_id_b)
    :ok
  end

  describe "data location registry with DAG ID scoping" do
    test "can register and lookup data locations for a specific DAG" do
      node = Node.self()
      :ok = DataLocationRegistry.register(@dag_id_a, :data1_dag_a, node)
      :ok = DataLocationRegistry.register(@dag_id_b, :data1_dag_b, node)

      assert {:ok, ^node} = DataLocationRegistry.lookup(@dag_id_a, :data1_dag_a)
      assert {:ok, ^node} = DataLocationRegistry.lookup(@dag_id_b, :data1_dag_b)
    end

    test "lookup returns :not_found for data in a different DAG" do
      node = Node.self()
      :ok = DataLocationRegistry.register(@dag_id_a, :data1_dag_a, node)

      # Try to look up data1_dag_a using dag_id_b
      assert {:error, :not_found} = DataLocationRegistry.lookup(@dag_id_b, :data1_dag_a)
      # Try to look up non-existent data in dag_id_a
      assert {:error, :not_found} = DataLocationRegistry.lookup(@dag_id_a, :non_existent_data)
    end

    test "can update registered locations for a specific DAG" do
      node1 = Node.self()
      node2 = :"other_node@example.com"

      :ok = DataLocationRegistry.register(@dag_id_a, :data_to_update, node1)
      assert {:ok, ^node1} = DataLocationRegistry.lookup(@dag_id_a, :data_to_update)

      # Update the location for the same DAG ID
      :ok = DataLocationRegistry.register(@dag_id_a, :data_to_update, node2)
      assert {:ok, ^node2} = DataLocationRegistry.lookup(@dag_id_a, :data_to_update)

      # Ensure an update for dag_id_a doesn't affect a hypothetical same data_id in dag_id_b
      # Store initial for dag_b
      :ok = DataLocationRegistry.register(@dag_id_b, :data_to_update, node1)
      # Update for dag_a
      :ok = DataLocationRegistry.register(@dag_id_a, :data_to_update, node2)
      # Verify dag_b is untouched
      assert {:ok, ^node1} = DataLocationRegistry.lookup(@dag_id_b, :data_to_update)
    end

    test "get_all returns locations only for the specified DAG" do
      node_a = Node.self()
      node_b = :"node_b@example.com"

      :ok = DataLocationRegistry.register(@dag_id_a, :item1_dag_a, node_a)
      :ok = DataLocationRegistry.register(@dag_id_a, :item2_dag_a, node_a)
      :ok = DataLocationRegistry.register(@dag_id_b, :item1_dag_b, node_b)

      locations_a = DataLocationRegistry.get_all(@dag_id_a)
      assert locations_a == %{item1_dag_a: node_a, item2_dag_a: node_a}
      assert map_size(locations_a) == 2

      locations_b = DataLocationRegistry.get_all(@dag_id_b)
      assert locations_b == %{item1_dag_b: node_b}
      assert map_size(locations_b) == 1

      assert DataLocationRegistry.get_all("non_existent_dag") == %{}
    end

    test "clear removes registrations only for the specified DAG" do
      node = Node.self()

      :ok = DataLocationRegistry.register(@dag_id_a, :data_a1, node)
      :ok = DataLocationRegistry.register(@dag_id_a, :data_a2, node)
      :ok = DataLocationRegistry.register(@dag_id_b, :data_b1, node)

      # Clear for dag_id_a
      :ok = DataLocationRegistry.clear(@dag_id_a)

      # Verify data for dag_id_a is gone
      assert {:error, :not_found} = DataLocationRegistry.lookup(@dag_id_a, :data_a1)
      assert map_size(DataLocationRegistry.get_all(@dag_id_a)) == 0

      # Verify data for dag_id_b still exists
      assert {:ok, ^node} = DataLocationRegistry.lookup(@dag_id_b, :data_b1)
      assert DataLocationRegistry.get_all(@dag_id_b) == %{data_b1: node}
    end
  end
end
