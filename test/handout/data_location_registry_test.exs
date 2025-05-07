defmodule Handout.DataLocationRegistryTest do
  use ExUnit.Case, async: false

  alias Handout.DataLocationRegistry

  setup do
    # Clear the registry between tests
    DataLocationRegistry.clear()

    :ok
  end

  describe "data location registry" do
    test "can register and lookup data locations" do
      # Register some data
      node = Node.self()
      other_node = :"other_node@example.com"

      :ok = DataLocationRegistry.register(:test_data1, node)
      :ok = DataLocationRegistry.register(:test_data2, other_node)

      # Lookup registered data
      assert {:ok, result1} = DataLocationRegistry.lookup(:test_data1)
      assert result1 == node

      assert {:ok, result2} = DataLocationRegistry.lookup(:test_data2)
      assert result2 == other_node
    end

    test "returns error when looking up unregistered data" do
      assert {:error, :not_found} = DataLocationRegistry.lookup(:nonexistent)
    end

    test "can update registered locations" do
      # Register data
      node = Node.self()
      new_node = :"new_node@example.com"

      :ok = DataLocationRegistry.register(:test_data3, node)
      {:ok, result} = DataLocationRegistry.lookup(:test_data3)
      assert result == node

      # Update the location
      :ok = DataLocationRegistry.register(:test_data3, new_node)
      {:ok, updated_result} = DataLocationRegistry.lookup(:test_data3)
      assert updated_result == new_node
    end

    test "can get all registered locations" do
      # Register multiple data locations
      node = Node.self()
      other_node = :"other_node@example.com"

      :ok = DataLocationRegistry.register(:test_data4, node)
      :ok = DataLocationRegistry.register(:test_data5, other_node)

      # Get all registrations
      all_locations = DataLocationRegistry.get_all()

      assert map_size(all_locations) == 2
      assert Map.get(all_locations, :test_data4) == node
      assert Map.get(all_locations, :test_data5) == other_node
    end

    test "clear removes all registrations" do
      # Register data
      :ok = DataLocationRegistry.register(:test_data6, Node.self())
      :ok = DataLocationRegistry.register(:test_data7, :"other_node@example.com")

      # Verify registration
      assert {:ok, _} = DataLocationRegistry.lookup(:test_data6)

      # Clear the registry
      :ok = DataLocationRegistry.clear()

      # Verify nothing is registered
      assert {:error, :not_found} = DataLocationRegistry.lookup(:test_data6)
      assert {:error, :not_found} = DataLocationRegistry.lookup(:test_data7)
      assert map_size(DataLocationRegistry.get_all()) == 0
    end
  end
end
