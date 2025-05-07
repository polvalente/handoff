defmodule Handout.ResultStoreTest do
  use ExUnit.Case, async: false

  alias Handout.{ResultStore, DataLocationRegistry}

  @dag_id "test_dag_id"

  setup do
    ResultStore.clear(@dag_id)
    DataLocationRegistry.clear(@dag_id)

    :ok
  end

  describe "result store operations" do
    test "stores and retrieves values" do
      # Store a value
      :ok = ResultStore.store(@dag_id, :test_id1, "test value 1")
      :ok = ResultStore.store(@dag_id, :test_id2, %{data: "complex value"})

      # Retrieve the values
      assert {:ok, "test value 1"} = ResultStore.get(@dag_id, :test_id1)
      assert {:ok, %{data: "complex value"}} = ResultStore.get(@dag_id, :test_id2)
    end

    test "returns error for non-existent values" do
      assert {:error, :not_found} = ResultStore.get(@dag_id, :nonexistent)
    end

    test "checks if values exist" do
      # Store a value
      :ok = ResultStore.store(@dag_id, :test_id3, 42)

      # Check existence
      assert ResultStore.has_value?(@dag_id, :test_id3) == true
      assert ResultStore.has_value?(@dag_id, :nonexistent) == false
    end

    test "clears all values for a dag" do
      # Store some values
      :ok = ResultStore.store(@dag_id, :test_id4, "value 4")
      :ok = ResultStore.store(@dag_id, :test_id5, "value 5")

      # Clear all values for the dag
      :ok = ResultStore.clear(@dag_id)

      # Check that values are gone
      assert {:error, :not_found} = ResultStore.get(@dag_id, :test_id4)
      assert {:error, :not_found} = ResultStore.get(@dag_id, :test_id5)
    end

    test "get_with_fetch retrieves local values" do
      # Store a value locally
      :ok = ResultStore.store(@dag_id, :test_id6, "local value")

      # Fetch should return the local value
      assert {:ok, "local value"} = ResultStore.get_with_fetch(@dag_id, :test_id6)
    end

    test "fetch_remote fails for unknown values" do
      # No value registered anywhere for this dag_id
      assert {:error, :not_found} = ResultStore.fetch_remote(@dag_id, :nonexistent)
    end
  end
end
