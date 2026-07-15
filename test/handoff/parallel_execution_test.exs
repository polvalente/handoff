defmodule Handoff.ParallelExecutionTest do
  @moduledoc """
  Deterministic tests for intra-DAG parallel scheduling via message-passing.

  Uses send/assert_receive barriers instead of wall-clock timing.
  """
  use ExUnit.Case, async: false

  alias Handoff.DAG
  alias Handoff.DistributedExecutor
  alias Handoff.Function
  alias Handoff.SimpleResourceTracker

  setup do
    SimpleResourceTracker.register(Node.self(), %{cpu: 8, memory: 8000, compute: 8})
    :ok
  end

  describe "Message-passing parallelism verification" do
    test "independent functions start concurrently (message-passing proof)" do
      dag_id = {self(), make_ref()}
      test_pid = self()

      dag =
        dag_id
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :func_a,
          args: [],
          code: &Handoff.DistributedTestFunctions.notify_and_wait/3,
          extra_args: [test_pid, :func_a, :result_a]
        })
        |> DAG.add_function(%Function{
          id: :func_b,
          args: [],
          code: &Handoff.DistributedTestFunctions.notify_and_wait/3,
          extra_args: [test_pid, :func_b, :result_b]
        })
        |> DAG.add_function(%Function{
          id: :func_c,
          args: [],
          code: &Handoff.DistributedTestFunctions.notify_and_wait/3,
          extra_args: [test_pid, :func_c, :result_c]
        })

      execute_task = Task.async(fn -> DistributedExecutor.execute(dag) end)

      pids =
        for func_id <- [:func_a, :func_b, :func_c] do
          assert_receive {:started, ^func_id, pid}, 1000
          pid
        end

      Enum.each(pids, fn pid -> send(pid, :continue) end)

      {:ok, result} = Task.await(execute_task, 5000)

      assert result.results[:func_a] == :result_a
      assert result.results[:func_b] == :result_b
      assert result.results[:func_c] == :result_c
    end

    test "dependent functions start in order (message-passing proof)" do
      dag_id = {self(), make_ref()}
      test_pid = self()

      dag =
        dag_id
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :step_a,
          args: [],
          code: &Handoff.DistributedTestFunctions.notify_start_and_complete/3,
          extra_args: [test_pid, :step_a, :done_a]
        })
        |> DAG.add_function(%Function{
          id: :step_b,
          args: [:step_a],
          code: &Handoff.DistributedTestFunctions.notify_start_and_complete_with_dep/4,
          extra_args: [test_pid, :step_b, :done_b]
        })
        |> DAG.add_function(%Function{
          id: :step_c,
          args: [:step_b],
          code: &Handoff.DistributedTestFunctions.notify_start_and_complete_with_dep/4,
          extra_args: [test_pid, :step_c, :done_c]
        })

      {:ok, result} = DistributedExecutor.execute(dag)

      messages = collect_all_messages()

      a_start_idx = Enum.find_index(messages, &(&1 == {:started, :step_a}))
      b_start_idx = Enum.find_index(messages, &(&1 == {:started, :step_b}))
      c_start_idx = Enum.find_index(messages, &(&1 == {:started, :step_c}))

      assert a_start_idx < b_start_idx,
             "Expected step_a to start before step_b. Messages: #{inspect(messages)}"

      assert b_start_idx < c_start_idx,
             "Expected step_b to start before step_c. Messages: #{inspect(messages)}"

      a_complete_idx = Enum.find_index(messages, &(&1 == {:completed, :step_a}))

      assert a_complete_idx < b_start_idx,
             "Expected step_a to complete before step_b starts. Messages: #{inspect(messages)}"

      assert result.results[:step_a] == :done_a
      assert result.results[:step_b] == :done_b
      assert result.results[:step_c] == :done_c
    end

    test "diamond DAG middle layer starts concurrently (message-passing proof)" do
      dag_id = {self(), make_ref()}
      test_pid = self()

      dag =
        dag_id
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :source,
          args: [],
          code: &Handoff.DistributedTestFunctions.notify_start_and_complete/3,
          extra_args: [test_pid, :source, :from_source]
        })
        |> DAG.add_function(%Function{
          id: :left,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.notify_and_wait_with_dep/4,
          extra_args: [test_pid, :left, :from_left]
        })
        |> DAG.add_function(%Function{
          id: :middle,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.notify_and_wait_with_dep/4,
          extra_args: [test_pid, :middle, :from_middle]
        })
        |> DAG.add_function(%Function{
          id: :right,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.notify_and_wait_with_dep/4,
          extra_args: [test_pid, :right, :from_right]
        })
        |> DAG.add_function(%Function{
          id: :sink,
          args: [:left, :middle, :right],
          code: &Handoff.DistributedTestFunctions.h/3,
          extra_args: []
        })

      execute_task = Task.async(fn -> DistributedExecutor.execute(dag) end)

      assert_receive {:started, :source}, 1000
      assert_receive {:completed, :source}, 1000

      pids =
        for func_id <- [:left, :middle, :right] do
          assert_receive {:started, ^func_id, pid}, 1000
          pid
        end

      Enum.each(pids, fn pid -> send(pid, :continue) end)

      {:ok, result} = Task.await(execute_task, 5000)

      assert result.results[:source] == :from_source
      assert result.results[:left] == :from_left
      assert result.results[:middle] == :from_middle
      assert result.results[:right] == :from_right
      assert result.results[:sink] == [:from_left, :from_middle, :from_right]
    end
  end

  describe "bounded concurrency" do
    test "max_concurrency limits simultaneous ready functions" do
      dag_id = {self(), make_ref()}
      test_pid = self()

      dag =
        Enum.reduce([:a, :b, :c], DAG.new(dag_id), fn id, acc ->
          DAG.add_function(acc, %Function{
            id: id,
            args: [],
            code: &Handoff.DistributedTestFunctions.notify_and_wait/3,
            extra_args: [test_pid, id, id]
          })
        end)

      execute_task =
        Task.async(fn -> DistributedExecutor.execute(dag, max_concurrency: 1) end)

      assert_receive {:started, first_id, first_pid}, 1000
      refute_receive {:started, _, _}, 100

      send(first_pid, :continue)

      assert_receive {:started, second_id, second_pid}, 1000
      assert second_id != first_id
      refute_receive {:started, _, _}, 100

      send(second_pid, :continue)

      assert_receive {:started, third_id, third_pid}, 1000
      assert third_id not in [first_id, second_id]
      send(third_pid, :continue)

      assert {:ok, _} = Task.await(execute_task, 5000)
    end
  end

  describe "fail-fast and cancellation" do
    test "function failure does not start downstream dependents" do
      dag_id = {self(), make_ref()}
      test_pid = self()

      dag =
        dag_id
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :boom,
          args: [],
          code: &Handoff.DistributedTestFunctions.raise_error/2,
          extra_args: ["boom"]
        })
        |> DAG.add_function(%Function{
          id: :downstream,
          args: [:boom],
          code: &Handoff.DistributedTestFunctions.notify_and_wait_with_dep/4,
          extra_args: [test_pid, :downstream, :should_not_run]
        })

      assert {:error, {:function_failed, :boom, reason}} =
               DistributedExecutor.execute(dag, max_retries: 0)

      assert is_binary(reason) or match?({_, _}, reason)
      refute_receive {:started, :downstream, _}, 200
    end

    test "parallel branch failure cancels siblings and skips sink" do
      dag_id = {self(), make_ref()}
      test_pid = self()

      dag =
        dag_id
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :ok_branch,
          args: [],
          code: &Handoff.DistributedTestFunctions.notify_and_wait/3,
          extra_args: [test_pid, :ok_branch, :ok]
        })
        |> DAG.add_function(%Function{
          id: :boom_branch,
          args: [],
          code: &Handoff.DistributedTestFunctions.crash_after_notify/2,
          extra_args: [test_pid, :boom_branch]
        })
        |> DAG.add_function(%Function{
          id: :sink,
          args: [:ok_branch, :boom_branch],
          code: &Handoff.DistributedTestFunctions.g/2,
          extra_args: []
        })

      execute_task =
        Task.async(fn -> DistributedExecutor.execute(dag, max_retries: 0) end)

      assert_receive {:started, :ok_branch, ok_pid}, 1000
      assert_receive {:started, :boom_branch, _boom_pid}, 1000

      assert {:error, {:function_failed, :boom_branch, _}} =
               Task.await(execute_task, 5000)

      # Sibling may have been killed while waiting; sink must not start.
      refute_receive {:started, :sink, _}, 100
      # Unblock mailbox if sibling still alive (should not be needed after cancel)
      send(ok_pid, :continue)
    end

    test "retries exhausted returns function_failed" do
      {:ok, agent} = Agent.start_link(fn -> %{count: 0} end)

      dag =
        {self(), make_ref()}
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :source,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [1]
        })
        |> DAG.add_function(%Function{
          id: :always_fails,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.always_failing_function/2,
          extra_args: [agent]
        })

      assert {:error, {:function_failed, :always_fails, _}} =
               DistributedExecutor.execute(dag, max_retries: 2)

      assert Agent.get(agent, & &1.count) == 3
    end

    test "per-function max_retries overrides execute-level default" do
      {:ok, agent} = Agent.start_link(fn -> %{count: 0} end)

      dag =
        {self(), make_ref()}
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :source,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [1]
        })
        |> DAG.add_function(%Function{
          id: :always_fails,
          args: [:source],
          code: &Handoff.DistributedTestFunctions.always_failing_function/2,
          extra_args: [agent],
          # Explicit 0 must not inherit the execute-level default of 5
          max_retries: 0
        })

      assert {:error, {:function_failed, :always_fails, _}} =
               DistributedExecutor.execute(dag, max_retries: 5)

      assert Agent.get(agent, & &1.count) == 1
    end

    test "caller death releases claimed resources" do
      cost = %{compute: 2}
      SimpleResourceTracker.register(Node.self(), %{compute: 2})

      dag =
        {self(), make_ref()}
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :hold,
          args: [],
          code: &Handoff.DistributedTestFunctions.notify_and_wait/3,
          extra_args: [self(), :hold, :done],
          cost: cost
        })

      caller =
        spawn(fn ->
          DistributedExecutor.execute(dag)
        end)

      assert_receive {:started, :hold, _pid}, 1000
      refute SimpleResourceTracker.available?(Node.self(), cost)

      Process.exit(caller, :kill)

      # DAGRunner monitors caller and releases claims
      assert_resource_available(Node.self(), cost, 1000)
    end

    test "successful DAG releases resources" do
      cost = %{compute: 1}
      SimpleResourceTracker.register(Node.self(), %{compute: 2})

      dag =
        {self(), make_ref()}
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :work,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [:ok],
          cost: cost
        })

      assert {:ok, _} = DistributedExecutor.execute(dag)
      assert SimpleResourceTracker.available?(Node.self(), %{compute: 2})
    end

    test "failed DAG releases resources" do
      cost = %{compute: 1}
      SimpleResourceTracker.register(Node.self(), %{compute: 2})

      dag =
        {self(), make_ref()}
        |> DAG.new()
        |> DAG.add_function(%Function{
          id: :boom,
          args: [],
          code: &Handoff.DistributedTestFunctions.raise_error/2,
          extra_args: ["fail"],
          cost: cost
        })

      assert {:error, {:function_failed, :boom, _}} =
               DistributedExecutor.execute(dag, max_retries: 0)

      assert SimpleResourceTracker.available?(Node.self(), %{compute: 2})
    end
  end

  defp collect_all_messages(acc \\ []) do
    receive do
      msg -> collect_all_messages([msg | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end

  defp assert_resource_available(node, req, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    fn ->
      if SimpleResourceTracker.available?(node, req) do
        true
      else
        Process.sleep(20)
        false
      end
    end
    |> Stream.repeatedly()
    |> Enum.find(fn
      true -> true
      false -> System.monotonic_time(:millisecond) >= deadline
    end)
    |> then(fn
      true ->
        assert SimpleResourceTracker.available?(node, req)

      false ->
        flunk("Resources #{inspect(req)} not available on #{inspect(node)} within #{timeout}ms")
    end)
  end
end
