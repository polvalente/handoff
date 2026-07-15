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

    def double(value), do: value * 2
    def triple(value), do: value * 3
    def pair(a, b), do: {a, b}
    def identity(value), do: value

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
end
