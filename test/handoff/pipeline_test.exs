defmodule Handoff.PipelineTest do
  use ExUnit.Case, async: false

  alias Handoff.DAG
  alias Handoff.Function
  alias Handoff.Pipeline

  defmodule StreamHelpers do
    @moduledoc false
    def load_store(agent) do
      Agent.update(agent, fn s -> %{s | init_count: s.init_count + 1} end)
      Agent.get(agent, & &1.payload)
    end

    def transform(state, value) do
      {state * value, state}
    end

    def transform_and_notify(state, value, test_pid) do
      send(test_pid, {:transformed, self(), value})
      {state * value, state}
    end

    def double(value), do: value * 2
    def triple(value), do: value * 3
    def pair(a, b), do: {a, b}
    def identity(value), do: value

    def slow_identity(value, delay_ms) do
      Process.sleep(delay_ms)
      value
    end

    def notify_and_wait_double(value, test_pid, function_id) do
      send(test_pid, {:started, function_id, self()})

      receive do
        :continue -> :ok
      after
        5_000 -> raise "timeout waiting for :continue in #{function_id}"
      end

      value * 2
    end

    def notify_and_wait_triple(value, test_pid, function_id) do
      send(test_pid, {:started, function_id, self()})

      receive do
        :continue -> :ok
      after
        5_000 -> raise "timeout waiting for :continue in #{function_id}"
      end

      value * 3
    end

    def notify_pair(a, b, test_pid, function_id) do
      send(test_pid, {:started, function_id, self()})
      {a, b}
    end

    def batch_double(batch) do
      Enum.map(batch, fn {value} -> value * 2 end)
    end

    def batch_double_counting(batch, agent) do
      Agent.update(agent, fn s ->
        %{s | calls: s.calls + 1, batches: s.batches ++ [batch]}
      end)

      Enum.map(batch, fn {value} -> value * 2 end)
    end

    def batch_wrong_length(_batch), do: [:only_one]
  end

  describe "Handoff.stream/2" do
    test "starts a pipeline and returns a handle without blocking" do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :out,
          args: [:source],
          code: &StreamHelpers.identity/1
        })

      assert {:ok, %Pipeline{} = handle} = Handoff.stream(dag)
      assert is_pid(handle.coordinator)
      assert Process.alive?(handle.coordinator)
      assert :ok = Pipeline.stop(handle)
    end
  end

  describe "fan-out / fan-in" do
    test "round-trips items through a multi-node DAG in push order" do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :left,
          args: [:source],
          code: &StreamHelpers.double/1
        })
        |> DAG.add_function(%Function{
          id: :right,
          args: [:source],
          code: &StreamHelpers.triple/1
        })
        |> DAG.add_function(%Function{
          id: :sink,
          args: [:left, :right],
          code: &StreamHelpers.pair/2
        })

      {:ok, handle} = Pipeline.start(dag)

      # Non-monotonic values so output order is not confused with sorted-by-value
      inputs = [3, 1, 4]
      n = length(inputs)

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(n)
        end)

      pushes =
        Enum.map(inputs, fn value ->
          assert {:ok, cid} = Pipeline.push(handle, value)
          {cid, value}
        end)

      # Correlation ids are assigned in push order (0..n-1)
      assert Enum.map(pushes, fn {cid, _} -> cid end) == Enum.to_list(0..(n - 1))

      outputs = Task.await(collect, 2_000)

      # Diamond join emits in push order: {double(x), triple(x)} per item
      assert outputs == Enum.map(inputs, &{&1 * 2, &1 * 3})
      assert outputs == [{6, 9}, {2, 3}, {8, 12}]

      assert :ok = Pipeline.stop(handle)
    end

    test "left and right overlap concurrently and always finish before sink" do
      test_pid = self()

      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :left,
          args: [:source],
          code: &StreamHelpers.notify_and_wait_double/3,
          extra_args: [test_pid, :left]
        })
        |> DAG.add_function(%Function{
          id: :right,
          args: [:source],
          code: &StreamHelpers.notify_and_wait_triple/3,
          extra_args: [test_pid, :right]
        })
        |> DAG.add_function(%Function{
          id: :sink,
          args: [:left, :right],
          code: &StreamHelpers.notify_pair/4,
          extra_args: [test_pid, :sink]
        })

      {:ok, handle} = Pipeline.start(dag)

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(2)
        end)

      # First item: release left then right
      assert {:ok, 0} = Pipeline.push(handle, 7)

      assert_receive {:started, :left, left_pid}, 1_000
      assert_receive {:started, :right, right_pid}, 1_000
      assert left_pid != right_pid
      refute_received {:started, :sink, _}

      send(left_pid, :continue)
      refute_received {:started, :sink, _}
      send(right_pid, :continue)

      assert_receive {:started, :sink, _}, 1_000

      # Second item: release right then left — sink still waits for both
      assert {:ok, 1} = Pipeline.push(handle, 5)

      assert_receive {:started, :left, left_pid}, 1_000
      assert_receive {:started, :right, right_pid}, 1_000
      assert left_pid != right_pid
      refute_received {:started, :sink, _}

      send(right_pid, :continue)
      refute_received {:started, :sink, _}
      send(left_pid, :continue)

      assert_receive {:started, :sink, _}, 1_000
      assert Task.await(collect, 2_000) == [{14, 21}, {10, 15}]

      assert :ok = Pipeline.stop(handle)
    end
  end

  describe "setup once, process many" do
    test "init runs exactly once while N items are processed in push order" do
      {:ok, agent} = Agent.start_link(fn -> %{init_count: 0, payload: 10} end)

      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :scale,
          args: [:source],
          init: {__MODULE__.StreamHelpers, :load_store, [agent]},
          code: &StreamHelpers.transform/2
        })

      {:ok, handle} = Pipeline.start(dag)

      # Non-monotonic values so output order is not confused with sorted-by-value
      inputs = [3, 1, 4, 1, 5]
      n = length(inputs)

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(n)
        end)

      pushes =
        Enum.map(inputs, fn value ->
          assert {:ok, cid} = Pipeline.push(handle, value)
          {cid, value}
        end)

      # Correlation ids are assigned in push order (0..n-1)
      assert Enum.map(pushes, fn {cid, _} -> cid end) == Enum.to_list(0..(n - 1))

      outputs = Task.await(collect, 2_000)

      # Stream emits in push/correlation order: each item scaled by the once-loaded state (10)
      assert outputs == Enum.map(inputs, &(&1 * 10))
      assert outputs == [30, 10, 40, 10, 50]

      assert Agent.get(agent, & &1.init_count) == 1
      assert :ok = Pipeline.stop(handle)
    end
  end

  describe "push-order preservation" do
    test "output order matches push order under concurrent pushes" do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :out,
          args: [:source],
          code: &StreamHelpers.identity/1
        })

      {:ok, handle} = Pipeline.start(dag)

      n = 20

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(n)
        end)

      # Give the stream consumer time to subscribe and propagate demand
      Process.sleep(20)

      results =
        1..n
        |> Task.async_stream(
          fn i ->
            {:ok, cid} = Pipeline.push(handle, i)
            {cid, i}
          end,
          max_concurrency: n,
          ordered: false
        )
        |> Enum.map(fn {:ok, pair} -> pair end)

      by_cid = Map.new(results)
      expected = Enum.map(0..(n - 1), &Map.fetch!(by_cid, &1))

      assert Task.await(collect, 5_000) == expected
      assert :ok = Pipeline.stop(handle)
    end
  end

  describe "teardown" do
    test "stop/1 tears down all stage processes" do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :out,
          args: [:source],
          code: &StreamHelpers.identity/1
        })

      {:ok, handle} = Pipeline.start(dag)

      stage_pids = [handle.aggregator | Map.values(handle.stages)]
      Enum.each(stage_pids, fn pid -> assert Process.alive?(pid) end)

      assert :ok = Pipeline.stop(handle)

      # Allow terminate to finish stopping children
      Process.sleep(50)

      refute Process.alive?(handle.coordinator)
      Enum.each(stage_pids, fn pid -> refute Process.alive?(pid) end)
    end
  end

  describe "parallelism" do
    alias Handoff.Pipeline.Stage

    test "starts N workers each running :init once" do
      test_pid = self()
      {:ok, agent} = Agent.start_link(fn -> %{init_count: 0, payload: 10} end)

      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :scale,
          args: [:source],
          parallelism: 4,
          init: {__MODULE__.StreamHelpers, :load_store, [agent]},
          code: &StreamHelpers.transform_and_notify/3,
          extra_args: [test_pid]
        })

      {:ok, handle} = Pipeline.start(dag)
      workers = Stage.worker_pids(handle.stages[:scale])

      assert length(workers) == 4
      assert Enum.all?(workers, &Process.alive?/1)
      assert Agent.get(agent, & &1.init_count) == 4

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(8)
        end)

      # Push 0..7 so every worker gets two items under rem(cid, 4) partitioning
      inputs = Enum.to_list(0..7)

      pushes =
        Enum.map(inputs, fn v ->
          assert {:ok, cid} = Pipeline.push(handle, v)
          {cid, v}
        end)

      assert Task.await(collect, 2_000) == Enum.map(inputs, &(&1 * 10))
      assert Agent.get(agent, & &1.init_count) == 4

      # The rem(cid, 4) partitioning means effectively a round-robin worker
      # load balancing. We assert on this via Stream.cycle zipping.
      Enum.zip_with(pushes, Stream.cycle(workers), fn {_cid, value}, expected_worker ->
        assert_receive {:transformed, ^expected_worker, ^value}, 1_000
      end)

      refute_received {:transformed, _, _}

      assert :ok = Pipeline.stop(handle)
      Process.sleep(50)
      Enum.each(workers, fn pid -> refute Process.alive?(pid) end)
    end

    test "fan-in with parallel sink does not double-fire or lose deps" do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :left,
          args: [:source],
          code: &StreamHelpers.double/1
        })
        |> DAG.add_function(%Function{
          id: :right,
          args: [:source],
          code: &StreamHelpers.triple/1
        })
        |> DAG.add_function(%Function{
          id: :sink,
          args: [:left, :right],
          parallelism: 4,
          code: &StreamHelpers.pair/2
        })

      {:ok, handle} = Pipeline.start(dag)
      assert length(Stage.worker_pids(handle.stages[:sink])) == 4

      inputs = [3, 1, 4, 2, 5, 9, 6, 8]
      n = length(inputs)

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(n)
        end)

      Enum.each(inputs, fn value -> assert {:ok, _} = Pipeline.push(handle, value) end)

      assert Task.await(collect, 2_000) == Enum.map(inputs, &{&1 * 2, &1 * 3})
      assert :ok = Pipeline.stop(handle)
    end

    test "aggregator output order matches push order with parallelism > 1" do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :slow,
          args: [:source],
          parallelism: 4,
          code: &StreamHelpers.slow_identity/2,
          extra_args: [30]
        })

      {:ok, handle} = Pipeline.start(dag)

      # Non-monotonic values so order is not confused with sorted-by-value
      inputs = [3, 1, 4, 1, 5, 9, 2, 6]
      n = length(inputs)

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(n)
        end)

      Process.sleep(20)

      Enum.each(inputs, fn value -> assert {:ok, _} = Pipeline.push(handle, value) end)

      assert Task.await(collect, 5_000) == inputs
      assert :ok = Pipeline.stop(handle)
    end

    test "throughput improves with parallelism > 1 under concurrent load" do
      delay_ms = 80
      n = 8

      time_pipeline = fn parallelism ->
        dag =
          DAG.new()
          |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
          |> DAG.add_function(%Function{
            id: :slow,
            args: [:source],
            parallelism: parallelism,
            code: &StreamHelpers.slow_identity/2,
            extra_args: [delay_ms]
          })

        {:ok, handle} = Pipeline.start(dag)

        collect =
          Task.async(fn ->
            handle |> Pipeline.stream() |> Enum.take(n)
          end)

        Process.sleep(20)

        {micros, outputs} =
          :timer.tc(fn ->
            Enum.each(1..n, fn i -> assert {:ok, _} = Pipeline.push(handle, i) end)
            Task.await(collect, 30_000)
          end)

        assert outputs == Enum.to_list(1..n)
        assert :ok = Pipeline.stop(handle)
        micros
      end

      serial_us = time_pipeline.(1)
      parallel_us = time_pipeline.(4)

      # Serial is ~n*delay; parallel with 4 workers should be clearly faster
      assert parallel_us < serial_us * 0.7,
             "expected parallelism=4 (#{parallel_us}µs) to beat parallelism=1 (#{serial_us}µs)"
    end
  end

  describe "batching" do
    test "batch_size flushes at N items with one :code call and unbatches results" do
      {:ok, agent} = Agent.start_link(fn -> %{calls: 0, batches: []} end)

      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :batched,
          args: [:source],
          batch_size: 3,
          code: &StreamHelpers.batch_double_counting/2,
          extra_args: [agent]
        })

      {:ok, handle} = Pipeline.start(dag)
      inputs = [1, 2, 3, 4, 5, 6]

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(length(inputs))
        end)

      Process.sleep(20)
      Enum.each(inputs, fn v -> assert {:ok, _} = Pipeline.push(handle, v) end)

      assert Task.await(collect, 2_000) == Enum.map(inputs, &(&1 * 2))

      %{calls: calls, batches: batches} = Agent.get(agent, & &1)
      assert calls == 2
      assert batches == [[{1}, {2}, {3}], [{4}, {5}, {6}]]

      assert :ok = Pipeline.stop(handle)
    end

    test "batch_timeout flushes a partial batch when no more items arrive" do
      {:ok, agent} = Agent.start_link(fn -> %{calls: 0, batches: []} end)

      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :batched,
          args: [:source],
          batch_size: 10,
          batch_timeout: 50,
          code: &StreamHelpers.batch_double_counting/2,
          extra_args: [agent]
        })

      {:ok, handle} = Pipeline.start(dag)
      inputs = [7, 8]

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(length(inputs))
        end)

      Process.sleep(20)
      Enum.each(inputs, fn v -> assert {:ok, _} = Pipeline.push(handle, v) end)

      assert Task.await(collect, 2_000) == [14, 16]

      %{calls: calls, batches: batches} = Agent.get(agent, & &1)
      assert calls == 1
      assert batches == [[{7}, {8}]]

      assert :ok = Pipeline.stop(handle)
    end

    test "batch_timeout alone flushes when batch_size is nil" do
      {:ok, agent} = Agent.start_link(fn -> %{calls: 0, batches: []} end)

      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :batched,
          args: [:source],
          batch_size: nil,
          batch_timeout: 50,
          code: &StreamHelpers.batch_double_counting/2,
          extra_args: [agent]
        })

      {:ok, handle} = Pipeline.start(dag)
      inputs = [11, 12]

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(length(inputs))
        end)

      Process.sleep(20)
      Enum.each(inputs, fn v -> assert {:ok, _} = Pipeline.push(handle, v) end)

      assert Task.await(collect, 2_000) == [22, 24]

      %{calls: calls, batches: batches} = Agent.get(agent, & &1)
      assert calls == 1
      assert batches == [[{11}, {12}]]

      assert :ok = Pipeline.stop(handle)
    end

    test "batch_size wins when both knobs are set and the batch fills first" do
      {:ok, agent} = Agent.start_link(fn -> %{calls: 0, batches: []} end)

      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :batched,
          args: [:source],
          batch_size: 3,
          # Far longer than push + flush latency so size must win
          batch_timeout: 5_000,
          code: &StreamHelpers.batch_double_counting/2,
          extra_args: [agent]
        })

      {:ok, handle} = Pipeline.start(dag)
      inputs = [1, 2, 3]

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(length(inputs))
        end)

      Process.sleep(20)
      Enum.each(inputs, fn v -> assert {:ok, _} = Pipeline.push(handle, v) end)

      assert Task.await(collect, 2_000) == [2, 4, 6]

      %{calls: calls, batches: batches} = Agent.get(agent, & &1)
      assert calls == 1
      assert batches == [[{1}, {2}, {3}]]

      assert :ok = Pipeline.stop(handle)
    end

    test "batching with parallelism > 1 forms per-worker batches and preserves order" do
      {:ok, agent} = Agent.start_link(fn -> %{calls: 0, batches: []} end)

      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :batched,
          args: [:source],
          parallelism: 2,
          batch_size: 2,
          code: &StreamHelpers.batch_double_counting/2,
          extra_args: [agent]
        })

      {:ok, handle} = Pipeline.start(dag)
      assert length(Handoff.Pipeline.Stage.worker_pids(handle.stages[:batched])) == 2

      # cids 0,1,2,3 → rem(cid, 2) partitions to workers [0,1,0,1]
      inputs = [10, 20, 30, 40]

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(length(inputs))
        end)

      Process.sleep(20)
      Enum.each(inputs, fn v -> assert {:ok, _} = Pipeline.push(handle, v) end)

      assert Task.await(collect, 2_000) == Enum.map(inputs, &(&1 * 2))

      %{calls: calls, batches: batches} = Agent.get(agent, & &1)
      assert calls == 2

      # Each worker flushes its own size-2 batch; completion order is racy
      assert Enum.sort(batches) == [[{10}, {30}], [{20}, {40}]]

      assert :ok = Pipeline.stop(handle)
    end

    test "batching after an upstream parallelism > 1 node preserves aggregator order" do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :slow,
          args: [:source],
          parallelism: 4,
          code: &StreamHelpers.slow_identity/2,
          extra_args: [30]
        })
        |> DAG.add_function(%Function{
          id: :batched,
          args: [:slow],
          batch_size: 3,
          batch_timeout: 100,
          code: &StreamHelpers.batch_double/1
        })

      {:ok, handle} = Pipeline.start(dag)
      inputs = [3, 1, 4, 1, 5, 9]
      n = length(inputs)

      collect =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(n)
        end)

      Process.sleep(20)
      Enum.each(inputs, fn v -> assert {:ok, _} = Pipeline.push(handle, v) end)

      assert Task.await(collect, 5_000) == Enum.map(inputs, &(&1 * 2))
      assert :ok = Pipeline.stop(handle)
    end

    test "mismatched batched :code result length crashes the stage with a clear error" do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :batched,
          args: [:source],
          batch_size: 2,
          code: &StreamHelpers.batch_wrong_length/1
        })

      {:ok, handle} = Pipeline.start(dag)
      stage = handle.stages[:batched]
      ref = Process.monitor(stage)

      # Stream subscriber so demand flows; it may die with the pipeline
      _collect =
        Task.async(fn ->
          try do
            handle |> Pipeline.stream() |> Enum.take(2)
          catch
            :exit, _ -> :pipeline_exited
          end
        end)

      Process.sleep(20)
      assert {:ok, _} = Pipeline.push(handle, 1)
      assert {:ok, _} = Pipeline.push(handle, 2)

      assert_receive {:DOWN, ^ref, :process, ^stage, reason}, 2_000

      message =
        case reason do
          {%RuntimeError{message: msg}, _} -> msg
          {exception, _} when is_exception(exception) -> Exception.message(exception)
          other -> inspect(other)
        end

      assert message =~ "same-length" or message =~ "list of 2 results"
    end
  end
end
