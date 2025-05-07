defmodule Handout.DataLocationRegistryTest do
  use ExUnit.Case, async: false

  alias Handout.DataLocationRegistry

  @dag_id "test_dag_id"

  setup do
    # Clear the registry for the test dag_id between tests
    DataLocationRegistry.clear(@dag_id)
    # Ensure other_dag_id is also cleared if used, making it a string too
    DataLocationRegistry.clear("another_dag")

    :ok
  end

  describe "data location registry" do
    test "can register and lookup data locations" do
      # Register some data
      node = Node.self()
      other_node = :"other_node@example.com"

      :ok = DataLocationRegistry.register(@dag_id, :test_data1, node)
      :ok = DataLocationRegistry.register(@dag_id, :test_data2, other_node)

      # Lookup registered data
      assert {:ok, result1} = DataLocationRegistry.lookup(@dag_id, :test_data1)
      assert result1 == node

      assert {:ok, result2} = DataLocationRegistry.lookup(@dag_id, :test_data2)
      assert result2 == other_node
    end

    test "returns error when looking up unregistered data" do
      assert {:error, :not_found} = DataLocationRegistry.lookup(@dag_id, :nonexistent)
    end

    test "can update registered locations" do
      # Register data
      node = Node.self()
      new_node = :"new_node@example.com"

      :ok = DataLocationRegistry.register(@dag_id, :test_data3, node)
      {:ok, result} = DataLocationRegistry.lookup(@dag_id, :test_data3)
      assert result == node

      # Update the location
      :ok = DataLocationRegistry.register(@dag_id, :test_data3, new_node)
      {:ok, updated_result} = DataLocationRegistry.lookup(@dag_id, :test_data3)
      assert updated_result == new_node
    end

    test "can get all registered locations for a dag" do
      # Register multiple data locations for the current dag_id
      node = Node.self()
      other_node = :"other_node@example.com"
      other_dag_id = "another_dag"

      :ok = DataLocationRegistry.register(@dag_id, :test_data4, node)
      :ok = DataLocationRegistry.register(@dag_id, :test_data5, other_node)
      # Register data for another DAG to ensure get_all is scoped
      :ok = DataLocationRegistry.register(other_dag_id, :test_data_other_dag, node)

      # Get all registrations for the current dag_id
      all_locations = DataLocationRegistry.get_all(@dag_id)

      assert map_size(all_locations) == 2
      assert Map.get(all_locations, :test_data4) == node
      assert Map.get(all_locations, :test_data5) == other_node

      # Cleanup data for other_dag_id to keep tests isolated if run in parallel or state persists
      DataLocationRegistry.clear(other_dag_id)
    end

    test "clear removes all registrations for a dag" do
      # Register data
      node = Node.self()
      other_dag_id = "another_dag"

      :ok = DataLocationRegistry.register(@dag_id, :test_data6, node)
      :ok = DataLocationRegistry.register(@dag_id, :test_data7, :"other_node@example.com")
      # Register data for another DAG to ensure clear is scoped
      :ok = DataLocationRegistry.register(other_dag_id, :test_data_other_dag, node)

      # Verify registration for current dag
      assert {:ok, _} = DataLocationRegistry.lookup(@dag_id, :test_data6)

      # Clear the registry for the current dag_id
      :ok = DataLocationRegistry.clear(@dag_id)

      # Verify nothing is registered for the current dag_id
      assert {:error, :not_found} = DataLocationRegistry.lookup(@dag_id, :test_data6)
      assert {:error, :not_found} = DataLocationRegistry.lookup(@dag_id, :test_data7)
      assert map_size(DataLocationRegistry.get_all(@dag_id)) == 0

      # Verify data for other_dag_id still exists
      assert {:ok, ^node} = DataLocationRegistry.lookup(other_dag_id, :test_data_other_dag)
      # Cleanup data for other_dag_id
      DataLocationRegistry.clear(other_dag_id)
    end
  end
end
