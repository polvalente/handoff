defmodule Handoff.PipelineDistributedTest do
  use ExUnit.Case, async: false

  alias Handoff.DAG
  alias Handoff.DistributedTestFunctions, as: F
  alias Handoff.Function
  alias Handoff.Pipeline
  alias Handoff.SimpleResourceTracker

  setup do
    [node_2 | _] = Application.get_env(:handoff, :test_nodes)
    SimpleResourceTracker.register(Node.self(), %{cpu: 4, memory: 2000})
    :rpc.call(node_2, SimpleResourceTracker, :register, [node_2, %{cpu: 4, memory: 2000}])
    SimpleResourceTracker.register(node_2, %{cpu: 4, memory: 2000})

    %{node_2: node_2}
  end

  describe "resource claim / release for pipeline lifetime" do
    test "claims once at start and releases exactly once at stop", %{node_2: node_2} do
      # Fill local capacity so the second regular stage must claim on node_2.
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :local_heavy,
          args: [:source],
          code: &F.identity/1,
          cost: %{cpu: 1, memory: 1950}
        })
        |> DAG.add_function(%Function{
          id: :remote,
          args: [:local_heavy],
          code: &F.identity/1,
          cost: %{cpu: 1, memory: 2000}
        })

      assert SimpleResourceTracker.available?(Node.self(), %{cpu: 1, memory: 1950})
      assert SimpleResourceTracker.available?(node_2, %{cpu: 1, memory: 2000})

      assert {:ok, handle} = Handoff.stream(dag, nodes: [Node.self(), node_2])
      assert node(handle.stages.local_heavy) == Node.self()
      assert node(handle.stages.remote) == node_2

      refute SimpleResourceTracker.available?(Node.self(), %{memory: 100})
      refute SimpleResourceTracker.available?(node_2, %{memory: 100})

      assert :ok = Pipeline.stop(handle)
      assert SimpleResourceTracker.available?(Node.self(), %{cpu: 1, memory: 1950})
      assert SimpleResourceTracker.available?(node_2, %{cpu: 1, memory: 2000})

      # Second cycle must not leak claims
      assert {:ok, handle2} = Handoff.stream(dag, nodes: [Node.self(), node_2])
      assert node(handle2.stages.remote) == node_2
      refute SimpleResourceTracker.available?(node_2, %{memory: 100})
      assert :ok = Pipeline.stop(handle2)
      assert SimpleResourceTracker.available?(node_2, %{cpu: 1, memory: 2000})
    end
  end

  describe "cost-based remote allocation" do
    test "allocator spills a stage to node_2 when local capacity is exhausted", %{node_2: node_2} do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :local_heavy,
          args: [:source],
          code: &F.identity/1,
          cost: %{cpu: 1, memory: 1950}
        })
        |> DAG.add_function(%Function{
          id: :spilled,
          args: [:local_heavy],
          code: &F.tag_with_node/1,
          cost: %{cpu: 1, memory: 2000}
        })

      assert {:ok, handle} = Handoff.stream(dag, nodes: [Node.self(), node_2])

      assert node(handle.stages.local_heavy) == Node.self()
      assert node(handle.stages.spilled) == node_2
      # No silent local fallback for the spilled stage
      refute node(handle.stages.spilled) == Node.self()

      task =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(1)
        end)

      assert {:ok, _cid} = Pipeline.push(handle, 42)
      assert [{^node_2, 42}] = Task.await(task)

      assert :ok = Pipeline.stop(handle)
    end

    test "concurrent pushes preserve order across cost-allocated nodes", %{node_2: node_2} do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :local_heavy,
          args: [:source],
          code: &F.identity/1,
          cost: %{cpu: 1, memory: 1950}
        })
        |> DAG.add_function(%Function{
          id: :on_remote,
          args: [:local_heavy],
          code: &F.double/1,
          cost: %{cpu: 1, memory: 2000}
        })
        |> DAG.add_function(%Function{
          id: :on_local,
          args: [:on_remote],
          code: &F.tag_with_node/1,
          cost: %{cpu: 1, memory: 50}
        })

      assert {:ok, handle} = Handoff.stream(dag, nodes: [Node.self(), node_2])
      assert node(handle.stages.local_heavy) == Node.self()
      assert node(handle.stages.on_remote) == node_2
      assert node(handle.stages.on_local) == Node.self()

      n = 20

      task =
        Task.async(fn ->
          handle |> Pipeline.stream() |> Enum.take(n)
        end)

      Enum.each(1..n, fn i ->
        assert {:ok, _} = Pipeline.push(handle, i)
      end)

      local = Node.self()
      expected = Enum.map(1..n, fn i -> {local, i * 2} end)
      assert Task.await(task, 10_000) == expected

      assert :ok = Pipeline.stop(handle)
    end
  end

  describe "node-down mid-stream policy" do
    test "remote stage death tears down the pipeline and releases claims", %{node_2: node_2} do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{id: :source, args: [], code: nil, type: :input})
        |> DAG.add_function(%Function{
          id: :local_heavy,
          args: [:source],
          code: &F.identity/1,
          cost: %{cpu: 1, memory: 1950}
        })
        |> DAG.add_function(%Function{
          id: :remote,
          args: [:local_heavy],
          code: &F.identity/1,
          cost: %{cpu: 1, memory: 2000}
        })

      assert {:ok, handle} = Handoff.stream(dag, nodes: [Node.self(), node_2])
      assert node(handle.stages.remote) == node_2
      refute SimpleResourceTracker.available?(node_2, %{memory: 100})

      coordinator = handle.coordinator
      remote_pid = handle.stages.remote
      ref = Process.monitor(coordinator)

      true = :rpc.call(node_2, Process, :exit, [remote_pid, :kill])

      assert_receive {:DOWN, ^ref, :process, ^coordinator, _reason}, 5_000

      # Allow terminate to release; tracker monitor is a backup
      Process.sleep(100)
      assert SimpleResourceTracker.available?(node_2, %{cpu: 1, memory: 2000})
      assert SimpleResourceTracker.available?(Node.self(), %{cpu: 1, memory: 1950})
    end
  end
end
