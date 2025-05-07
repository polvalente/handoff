defmodule Handout.ResultStoreTest do
  use ExUnit.Case, async: false

  alias Handout.{ResultStore, DataLocationRegistry}

  @dag_id_a "test_dag_a"
  @dag_id_b "test_dag_b"

  setup do
    ResultStore.clear(@dag_id_a)
    ResultStore.clear(@dag_id_b)
    # Though not directly used by all RS tests, good practice
    DataLocationRegistry.clear(@dag_id_a)
    DataLocationRegistry.clear(@dag_id_b)
    :ok
  end

  describe "result store operations with DAG ID scoping" do
    test "stores and retrieves values for a specific DAG" do
      :ok = ResultStore.store(@dag_id_a, :item1, "value_a1")
      # Same item ID, different DAG
      :ok = ResultStore.store(@dag_id_b, :item1, "value_b1")

      assert {:ok, "value_a1"} = ResultStore.get(@dag_id_a, :item1)
      assert {:ok, "value_b1"} = ResultStore.get(@dag_id_b, :item1)
    end

    test "get returns :not_found for data in a different DAG or non-existent data" do
      :ok = ResultStore.store(@dag_id_a, :item_a, "value_a")

      assert {:error, :not_found} = ResultStore.get(@dag_id_b, :item_a)
      assert {:error, :not_found} = ResultStore.get(@dag_id_a, :non_existent_item)
    end

    test "has_value? is scoped by DAG ID" do
      :ok = ResultStore.store(@dag_id_a, :item_exists_a, 123)

      assert ResultStore.has_value?(@dag_id_a, :item_exists_a) == true
      assert ResultStore.has_value?(@dag_id_b, :item_exists_a) == false
      assert ResultStore.has_value?(@dag_id_a, :non_existent) == false
    end

    test "clears values only for the specified DAG" do
      :ok = ResultStore.store(@dag_id_a, :item_a_clear, "data_a")
      :ok = ResultStore.store(@dag_id_b, :item_b_clear, "data_b")

      :ok = ResultStore.clear(@dag_id_a)

      assert {:error, :not_found} = ResultStore.get(@dag_id_a, :item_a_clear)
      # Should still exist
      assert {:ok, "data_b"} = ResultStore.get(@dag_id_b, :item_b_clear)
    end

    test "get_with_fetch retrieves local values for a specific DAG" do
      :ok = ResultStore.store(@dag_id_a, :item_fetch_a, "local_a")
      # Same item ID
      :ok = ResultStore.store(@dag_id_b, :item_fetch_a, "local_b")

      assert {:ok, "local_a"} = ResultStore.get_with_fetch(@dag_id_a, :item_fetch_a)
      assert {:ok, "local_b"} = ResultStore.get_with_fetch(@dag_id_b, :item_fetch_a)
    end

    test "fetch_remote fails for unknown values within a specific DAG" do
      # Assuming DataLocationRegistry is empty or doesn't have :nonexistent for @dag_id_a
      # Register for other DAG
      DataLocationRegistry.register(@dag_id_b, :nonexistent, Node.self())

      assert {:error, :not_found} = ResultStore.fetch_remote(@dag_id_a, :nonexistent)
    end
  end
end
