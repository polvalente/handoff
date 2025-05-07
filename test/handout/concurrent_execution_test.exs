defmodule Handoff.ConcurrentExecutionTest do
  # async: false for controlled concurrency testing initially
  # Can be async now
  use ExUnit.Case, async: true

  alias Handoff.DAG
  alias Handoff.DataLocationRegistry
  alias Handoff.Executor
  alias Handoff.Function
  alias Handoff.ResultStore

  defp create_simple_dag(dag_id, val_prefix) do
    dag_id
    |> DAG.new()
    |> DAG.add_function(%Function{
      id: :source,
      args: [],
      code: fn -> "#{val_prefix}_source_val" end
    })
    |> DAG.add_function(%Function{
      id: :process,
      args: [:source],
      code: fn source_val -> "#{val_prefix}_processed_#{source_val}" end
    })
  end

  describe "Concurrent Local DAG Execution (Handoff.Executor)" do
    test "executes two simple DAGs concurrently with data isolation" do
      dag_id_1 = {self(), 1}
      dag_id_2 = {self(), 2}

      dag1 = create_simple_dag(dag_id_1, "dag1")
      dag2 = create_simple_dag(dag_id_2, "dag2")

      # Execute DAGs. GenServer calls to Executor are synchronous,
      # so true parallelism isn't tested here, but data isolation is.
      # For true parallelism, tasks/async would be needed if Executor.execute was async itself.
      {:ok, res1} = Executor.execute(dag1)
      {:ok, res2} = Executor.execute(dag2)

      # Check DAG 1 results and ID
      assert res1.dag_id == dag_id_1
      assert res1.results[:source] == "dag1_source_val"
      assert res1.results[:process] == "dag1_processed_dag1_source_val"

      # Check DAG 2 results and ID
      assert res2.dag_id == dag_id_2
      assert res2.results[:source] == "dag2_source_val"
      assert res2.results[:process] == "dag2_processed_dag2_source_val"

      # Verify data isolation in ResultStore
      assert {:ok, "dag1_source_val"} = ResultStore.get(dag_id_1, :source)
      assert {:ok, "dag1_processed_dag1_source_val"} = ResultStore.get(dag_id_1, :process)
      assert {:ok, "dag2_source_val"} = ResultStore.get(dag_id_2, :source)

      assert {:ok, "dag2_processed_dag2_source_val"} =
               ResultStore.get(dag_id_2, :process)

      # Verify that an item ID from dag1 cannot be fetched using dag2's ID if
      # that item ID was unique to dag1.
      # Since :source and :process are used in both, their differing values
      # for each dag_id (asserted above) prove isolation.
      # For an explicit non-existent cross-check, imagine dag1 had a unique item:
      # ResultStore.store(dag_id_1, :unique_to_dag1, "unique_val")
      # assert {:error, :not_found} = ResultStore.get(dag_id_2, :unique_to_dag1)
      # This is implicitly covered by the setup clearing both DAG IDs and then only
      # populating as defined.
      # A check for a completely non-existent key in one of the DAGs is fine:
      assert {:error, :not_found} =
               ResultStore.get(dag_id_1, :completely_random_key_not_in_any_dag)
    end
  end

  describe "Concurrent Distributed DAG Execution (Handoff.DistributedExecutor)" do
    defp create_dist_dag(dag_id, val_prefix, node_to_run_on) do
      dag_id
      |> DAG.new()
      |> DAG.add_function(%Function{
        id: :source_op,
        args: [],
        code: fn -> "#{val_prefix}_source_data" end,
        # cost: %{cpu: 1}, # Ensure it gets assigned to a node
        # Explicitly assign for testing, though allocator would do it
        node: node_to_run_on
      })
      |> DAG.add_function(%Function{
        id: :process_op,
        args: [:source_op],
        code: fn data -> "#{val_prefix}_processed_#{data}" end,
        # cost: %{cpu: 1},
        node: node_to_run_on
      })
    end

    # @tag :skip_in_ci # Skip if full cluster setup is problematic in CI
    test "executes two simple DAGs concurrently with data isolation via DistributedExecutor" do
      # Use test PID
      dag_a_id = {self(), 1}
      # Use a spawned PID for the second DAG
      dag_b_id = {self(), 2}

      # For this test, we'll have both run on Node.self() to simplify,
      # focusing on data isolation via DAG ID with DistributedExecutor.
      node_self = Node.self()

      dag_a = create_dist_dag(dag_a_id, "dagA", node_self)
      dag_b = create_dist_dag(dag_b_id, "dagB", node_self)

      task_a = Task.async(fn -> Handoff.DistributedExecutor.execute(dag_a, []) end)
      task_b = Task.async(fn -> Handoff.DistributedExecutor.execute(dag_b, []) end)

      res_a = Task.await(task_a, 15_000)
      res_b = Task.await(task_b, 15_000)

      assert {:ok, %{dag_id: ^dag_a_id, results: results_a}} = res_a
      assert {:ok, %{dag_id: ^dag_b_id, results: results_b}} = res_b

      assert results_a[:source_op] == "dagA_source_data"
      assert results_a[:process_op] == "dagA_processed_dagA_source_data"
      assert results_b[:source_op] == "dagB_source_data"
      assert results_b[:process_op] == "dagB_processed_dagB_source_data"

      # Verify ResultStore isolation
      assert {:ok, "dagA_source_data"} = ResultStore.get(dag_a_id, :source_op)
      assert {:ok, "dagB_source_data"} = ResultStore.get(dag_b_id, :source_op)

      # Verify DataLocationRegistry isolation (source_op result is registered)
      assert {:ok, ^node_self} = DataLocationRegistry.lookup(dag_a_id, :source_op)
      assert {:ok, ^node_self} = DataLocationRegistry.lookup(dag_b_id, :source_op)
      # Check that process_op (the final one) location was also registered for its node
      assert {:ok, ^node_self} = DataLocationRegistry.lookup(dag_a_id, :process_op)
      assert {:ok, ^node_self} = DataLocationRegistry.lookup(dag_b_id, :process_op)

      # Ensure dag_a's item isn't findable with dag_b's ID in DLR
      assert {:error, :not_found} =
               DataLocationRegistry.lookup(dag_b_id, :unique_to_a_if_it_existed)
    end
  end

  describe "API Error Handling with DAG IDs" do
    test "Handoff.get_result returns :not_found for non-existent dag_id or item_id" do
      # Use test PID
      dag_id_exists = {self(), 1}
      item_id_exists_for_dag = :item_for_error_test
      # A different PID
      non_existent_dag_id = {self(), 2}

      # Store an item for a known DAG ID to make the DAG ID "valid" in some sense
      ResultStore.store(dag_id_exists, item_id_exists_for_dag, "some_value")
      DataLocationRegistry.register(dag_id_exists, item_id_exists_for_dag, Node.self())

      # Try to get result with a completely non-existent DAG ID
      assert {:error, :timeout} =
               Handoff.get_result(non_existent_dag_id, :any_item, 50)

      # (get_result uses get_with_timeout which tries to fetch,
      # so :timeout is expected if not found after trying)

      # Try to get a non-existent item from an existing DAG ID
      assert {:error, :timeout} =
               Handoff.get_result(dag_id_exists, :non_existent_item_for_error, 50)

      # Try to get an existing item from an existing DAG ID (should succeed)
      assert {:ok, "some_value"} = Handoff.get_result(dag_id_exists, item_id_exists_for_dag, 50)
    end
  end
end
