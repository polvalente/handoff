defmodule Handout.ResultStoreTest do
  use ExUnit.Case, async: false

  alias Handout.{ResultStore, DataLocationRegistry}

  setup do
    ResultStore.clear()
    DataLocationRegistry.clear()

    :ok
  end

  describe "result store operations" do
    test "stores and retrieves values" do
      # Store a value
      :ok = ResultStore.store(:test_id1, "test value 1")
      :ok = ResultStore.store(:test_id2, %{data: "complex value"})

      # Retrieve the values
      assert {:ok, "test value 1"} = ResultStore.get(:test_id1)
      assert {:ok, %{data: "complex value"}} = ResultStore.get(:test_id2)
    end

    test "returns error for non-existent values" do
      assert {:error, :not_found} = ResultStore.get(:nonexistent)
    end

    test "checks if values exist" do
      # Store a value
      :ok = ResultStore.store(:test_id3, 42)

      # Check existence
      assert ResultStore.has_value?(:test_id3) == true
      assert ResultStore.has_value?(:nonexistent) == false
    end

    test "clears all values" do
      # Store some values
      :ok = ResultStore.store(:test_id4, "value 4")
      :ok = ResultStore.store(:test_id5, "value 5")

      # Clear all values
      :ok = ResultStore.clear()

      # Check that values are gone
      assert {:error, :not_found} = ResultStore.get(:test_id4)
      assert {:error, :not_found} = ResultStore.get(:test_id5)
    end

    test "get_with_fetch retrieves local values" do
      # Store a value locally
      :ok = ResultStore.store(:test_id6, "local value")

      # Fetch should return the local value
      assert {:ok, "local value"} = ResultStore.get_with_fetch(:test_id6)
    end

    test "fetch_remote fails for unknown values" do
      # No value registered anywhere
      assert {:error, :not_found} = ResultStore.fetch_remote(:nonexistent)
    end
  end
end
