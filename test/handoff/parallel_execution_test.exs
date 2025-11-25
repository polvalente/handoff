defmodule Handoff.ParallelExecutionTest do
  @moduledoc """
  Tests that prove parallel execution is working correctly by using timing.

  The key insight: if N independent operations each take T ms to execute,
  - Sequential execution takes N * T ms
  - Parallel execution takes ~T ms (plus small overhead)

  By measuring actual execution time, we can prove parallelism is happening.

  The DistributedExecutor supports parallel execution both:
  1. Across multiple DAGs (when launched concurrently via Task.async)
  2. Within a single DAG (independent functions with satisfied dependencies run in parallel)
  """
  use ExUnit.Case, async: false

  alias Handoff.DAG
  alias Handoff.DistributedExecutor
  alias Handoff.Function

  @sleep_duration_ms 100
  @parallel_tolerance_factor 2.5

  describe "Intra-DAG Parallel Execution" do
    test "independent functions within a DAG execute in parallel (timing proof)" do
      dag_id = {self(), make_ref()}

      # Create a DAG with 4 independent branches, each sleeping for @sleep_duration_ms
      # If parallel: total time ≈ @sleep_duration_ms
      # If sequential: total time ≈ 4 * @sleep_duration_ms = 400ms
      dag =
        dag_id
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :branch_a,
          args: [],
          code: &Handoff.DistributedTestFunctions.sleep_and_return/2,
          extra_args: [:result_a, @sleep_duration_ms]
        })
        |> DAG.add_function(%Function{
          id: :branch_b,
          args: [],
          code: &Handoff.DistributedTestFunctions.sleep_and_return/2,
          extra_args: [:result_b, @sleep_duration_ms]
        })
        |> DAG.add_function(%Function{
          id: :branch_c,
          args: [],
          code: &Handoff.DistributedTestFunctions.sleep_and_return/2,
          extra_args: [:result_c, @sleep_duration_ms]
        })
        |> DAG.add_function(%Function{
          id: :branch_d,
          args: [],
          code: &Handoff.DistributedTestFunctions.sleep_and_return/2,
          extra_args: [:result_d, @sleep_duration_ms]
        })

      # Measure execution time
      start_time = System.monotonic_time(:millisecond)
      {:ok, result} = DistributedExecutor.execute(dag)
      end_time = System.monotonic_time(:millisecond)
      elapsed_ms = end_time - start_time

      # Verify all results are correct
      assert result.results[:branch_a] == :result_a
      assert result.results[:branch_b] == :result_b
      assert result.results[:branch_c] == :result_c
      assert result.results[:branch_d] == :result_d

      # Calculate expected times
      sequential_time = 4 * @sleep_duration_ms
      parallel_time = @sleep_duration_ms

      # The actual time should be much closer to parallel_time than sequential_time
      max_allowed_time = parallel_time * @parallel_tolerance_factor

      assert elapsed_ms < max_allowed_time,
             "Execution took #{elapsed_ms}ms, but should be under #{max_allowed_time}ms " <>
               "if running in parallel. Sequential would take ~#{sequential_time}ms."

      # Also verify we're not impossibly fast (sanity check)
      assert elapsed_ms >= parallel_time * 0.8,
             "Execution too fast (#{elapsed_ms}ms), expected at least ~#{parallel_time}ms"
    end

    test "diamond DAG executes middle layer in parallel" do
      dag_id = {self(), make_ref()}

      # Create a diamond-shaped DAG:
      #       source (instant)
      #      /  |  \
      #     A   B   C  (each sleeps 100ms, should run in parallel)
      #      \  |  /
      #       sink (depends on A, B, C)
      #
      # If parallel middle layer: total ≈ @sleep_duration_ms + small overhead
      # If sequential middle layer: total ≈ 3 * @sleep_duration_ms
      dag =
        dag_id
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :source,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [:source_data]
        })
        |> DAG.add_function(%Function{
          id: :parallel_a,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.sleep_with_dep_and_return/3,
          extra_args: [:from_a, @sleep_duration_ms]
        })
        |> DAG.add_function(%Function{
          id: :parallel_b,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.sleep_with_dep_and_return/3,
          extra_args: [:from_b, @sleep_duration_ms]
        })
        |> DAG.add_function(%Function{
          id: :parallel_c,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.sleep_with_dep_and_return/3,
          extra_args: [:from_c, @sleep_duration_ms]
        })
        |> DAG.add_function(%Function{
          id: :sink,
          args: [:parallel_a, :parallel_b, :parallel_c],
          code: &Handoff.DistributedTestFunctions.h/3,
          extra_args: []
        })

      start_time = System.monotonic_time(:millisecond)
      {:ok, result} = DistributedExecutor.execute(dag)
      end_time = System.monotonic_time(:millisecond)
      elapsed_ms = end_time - start_time

      # Verify results
      assert result.results[:source] == :source_data
      assert result.results[:parallel_a] == :from_a
      assert result.results[:parallel_b] == :from_b
      assert result.results[:parallel_c] == :from_c
      assert result.results[:sink] == [:from_a, :from_b, :from_c]

      # Timing assertions - middle layer should run in parallel
      sequential_time = 3 * @sleep_duration_ms
      parallel_time = @sleep_duration_ms
      max_allowed_time = parallel_time * @parallel_tolerance_factor

      assert elapsed_ms < max_allowed_time,
             "Diamond DAG took #{elapsed_ms}ms, expected under #{max_allowed_time}ms " <>
               "(parallel middle layer). Sequential would take ~#{sequential_time}ms."
    end

    test "sequential dependencies execute sequentially (control test)" do
      dag_id = {self(), make_ref()}

      # Create a chain where each step depends on the previous
      # A -> B -> C (each sleeps 50ms)
      # This MUST be sequential, so total ≈ 3 * 50ms = 150ms
      sleep_ms = 50

      dag =
        dag_id
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :step_1,
          args: [],
          code: &Handoff.DistributedTestFunctions.sleep_and_return/2,
          extra_args: [:step_1_done, sleep_ms]
        })
        |> DAG.add_function(%Function{
          id: :step_2,
          args: [:step_1],
          code: &Handoff.DistributedTestFunctions.sleep_with_dep_and_return/3,
          extra_args: [:step_2_done, sleep_ms]
        })
        |> DAG.add_function(%Function{
          id: :step_3,
          args: [:step_2],
          code: &Handoff.DistributedTestFunctions.sleep_with_dep_and_return/3,
          extra_args: [:step_3_done, sleep_ms]
        })

      start_time = System.monotonic_time(:millisecond)
      {:ok, result} = DistributedExecutor.execute(dag)
      end_time = System.monotonic_time(:millisecond)
      elapsed_ms = end_time - start_time

      # Verify results
      assert result.results[:step_1] == :step_1_done
      assert result.results[:step_2] == :step_2_done
      assert result.results[:step_3] == :step_3_done

      # Sequential execution should take at least sum of all sleeps
      min_expected_time = 3 * sleep_ms

      assert elapsed_ms >= min_expected_time * 0.9,
             "Sequential chain took #{elapsed_ms}ms, expected at least ~#{min_expected_time}ms"
    end

    test "mixed parallel and sequential paths" do
      dag_id = {self(), make_ref()}

      # Create a DAG with two parallel paths of different lengths:
      #
      # Path 1: A (100ms) -> B (100ms) = 200ms total
      # Path 2: C (100ms)             = 100ms total
      #
      # Both paths run in parallel, so total should be ~200ms (the longer path)
      # NOT 300ms (if everything were sequential)

      dag =
        dag_id
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :path1_step1,
          args: [],
          code: &Handoff.DistributedTestFunctions.sleep_and_return/2,
          extra_args: [:p1_s1, @sleep_duration_ms]
        })
        |> DAG.add_function(%Function{
          id: :path1_step2,
          args: [:path1_step1],
          code: &Handoff.DistributedTestFunctions.sleep_with_dep_and_return/3,
          extra_args: [:p1_s2, @sleep_duration_ms]
        })
        |> DAG.add_function(%Function{
          id: :path2_single,
          args: [],
          code: &Handoff.DistributedTestFunctions.sleep_and_return/2,
          extra_args: [:p2, @sleep_duration_ms]
        })

      start_time = System.monotonic_time(:millisecond)
      {:ok, result} = DistributedExecutor.execute(dag)
      end_time = System.monotonic_time(:millisecond)
      elapsed_ms = end_time - start_time

      # Verify results
      assert result.results[:path1_step1] == :p1_s1
      assert result.results[:path1_step2] == :p1_s2
      assert result.results[:path2_single] == :p2

      # Critical path is path1 with 2 sequential steps = 200ms
      # Path2 runs in parallel with path1_step1, so it doesn't add time
      critical_path_time = 2 * @sleep_duration_ms
      max_allowed_time = critical_path_time * 1.5

      # If not parallel, would take 300ms
      sequential_time = 3 * @sleep_duration_ms

      assert elapsed_ms < max_allowed_time,
             "Mixed paths took #{elapsed_ms}ms, expected under #{max_allowed_time}ms " <>
               "(critical path = #{critical_path_time}ms). Sequential would take ~#{sequential_time}ms."

      assert elapsed_ms >= critical_path_time * 0.9,
             "Too fast (#{elapsed_ms}ms), critical path requires ~#{critical_path_time}ms"
    end
  end

  describe "Inter-DAG Parallel Execution" do
    test "multiple DAGs execute in parallel (timing proof)" do
      # Create 4 independent DAGs, each with a single function that sleeps 100ms
      # If parallel: total time ≈ 100ms
      # If sequential: total time ≈ 400ms
      dag1 = create_sleep_dag({self(), 1}, :result_1)
      dag2 = create_sleep_dag({self(), 2}, :result_2)
      dag3 = create_sleep_dag({self(), 3}, :result_3)
      dag4 = create_sleep_dag({self(), 4}, :result_4)

      # Measure execution time with parallel DAG execution
      start_time = System.monotonic_time(:millisecond)

      # Launch all 4 DAGs in parallel
      task1 = Task.async(fn -> DistributedExecutor.execute(dag1) end)
      task2 = Task.async(fn -> DistributedExecutor.execute(dag2) end)
      task3 = Task.async(fn -> DistributedExecutor.execute(dag3) end)
      task4 = Task.async(fn -> DistributedExecutor.execute(dag4) end)

      # Wait for all to complete
      {:ok, res1} = Task.await(task1, 15_000)
      {:ok, res2} = Task.await(task2, 15_000)
      {:ok, res3} = Task.await(task3, 15_000)
      {:ok, res4} = Task.await(task4, 15_000)

      end_time = System.monotonic_time(:millisecond)
      elapsed_ms = end_time - start_time

      # Verify all results are correct
      assert res1.results[:sleep_func] == :result_1
      assert res2.results[:sleep_func] == :result_2
      assert res3.results[:sleep_func] == :result_3
      assert res4.results[:sleep_func] == :result_4

      # Calculate expected times
      sequential_time = 4 * @sleep_duration_ms
      parallel_time = @sleep_duration_ms

      # The actual time should be much closer to parallel_time than sequential_time
      max_allowed_time = parallel_time * @parallel_tolerance_factor

      assert elapsed_ms < max_allowed_time,
             "Execution took #{elapsed_ms}ms, but should be under #{max_allowed_time}ms " <>
               "if running in parallel. Sequential would take ~#{sequential_time}ms."
    end

    test "parallel speedup ratio demonstrates actual parallelism" do
      # This test quantifies the speedup from parallel execution
      # Speedup = Sequential Time / Parallel Time
      # For true parallelism with N tasks: speedup ≈ N

      num_dags = 4

      # Create DAGs
      dags =
        Enum.map(1..num_dags, fn i ->
          create_sleep_dag({self(), i}, :"result_#{i}")
        end)

      # Measure parallel execution time
      parallel_start = System.monotonic_time(:millisecond)

      tasks = Enum.map(dags, fn dag -> Task.async(fn -> DistributedExecutor.execute(dag) end) end)
      results = Enum.map(tasks, fn task -> Task.await(task, 15_000) end)

      parallel_end = System.monotonic_time(:millisecond)
      parallel_time = parallel_end - parallel_start

      # Verify all succeeded
      Enum.each(results, fn result ->
        assert {:ok, _} = result
      end)

      # Measure sequential execution time (new DAGs to avoid caching effects)
      sequential_dags =
        Enum.map(1..num_dags, fn i ->
          create_sleep_dag({self(), 100 + i}, :"seq_result_#{i}")
        end)

      sequential_start = System.monotonic_time(:millisecond)

      Enum.each(sequential_dags, fn dag ->
        {:ok, _} = DistributedExecutor.execute(dag)
      end)

      sequential_end = System.monotonic_time(:millisecond)
      sequential_time = sequential_end - sequential_start

      # Calculate speedup
      speedup = sequential_time / parallel_time

      # With 4 parallel tasks, we expect speedup > 2 (being conservative)
      assert speedup > 2.0,
             "Speedup ratio #{Float.round(speedup, 2)}x is too low. " <>
               "Sequential: #{sequential_time}ms, Parallel: #{parallel_time}ms. " <>
               "Expected at least 2x speedup for #{num_dags} parallel DAGs."

      # Log the actual speedup for visibility
      IO.puts(
        "\n  Parallel execution speedup: #{Float.round(speedup, 2)}x " <>
          "(Sequential: #{sequential_time}ms, Parallel: #{parallel_time}ms)"
      )
    end
  end

  # Helper function to create a simple DAG with one sleep function
  defp create_sleep_dag(dag_id, return_value) do
    dag_id
    |> DAG.new()
    |> DAG.add_function(%Function{
      id: :sleep_func,
      args: [],
      code: &Handoff.DistributedTestFunctions.sleep_and_return/2,
      extra_args: [return_value, @sleep_duration_ms]
    })
  end
end
