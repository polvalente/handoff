defmodule Handoff.ResourceTest do
  use ExUnit.Case, async: false

  alias Handoff.Function
  alias Handoff.SimpleAllocator
  alias Handoff.SimpleResourceTracker

  setup do
    # Get test nodes if available
    test_nodes = Application.get_env(:handoff, :test_nodes, [])

    # Register code paths on remote nodes for module visibility
    for node <- test_nodes do
      :erpc.call(node, :code, :add_paths, [:code.get_path()])
    end

    {:ok, %{test_nodes: test_nodes}}
  end

  describe "process monitoring and resource cleanup" do
    test "automatically releases resources when process dies" do
      node = Node.self()
      resources = %{cpu: 2, memory: 1000}

      # Register the node with resources
      SimpleResourceTracker.register(node, resources)

      # Verify initial state - all resources available
      assert SimpleResourceTracker.available?(node, %{cpu: 2, memory: 1000})

      # Spawn a process that will request resources and then die
      test_pid = self()

      spawned_pid =
        spawn(fn ->
          # Request resources from the spawned process
          :ok = SimpleResourceTracker.request(node, %{cpu: 1, memory: 500})

          # Notify test process that resources were allocated
          send(test_pid, :resources_allocated)

          # Wait a bit then exit (simulating process death)
          :timer.sleep(100)
        end)

      # Wait for resources to be allocated
      assert_receive :resources_allocated, 1000

      # Verify resources are now in use
      refute SimpleResourceTracker.available?(node, %{cpu: 2, memory: 1000})
      assert SimpleResourceTracker.available?(node, %{cpu: 1, memory: 500})

      # Wait for the process to die
      ref = Process.monitor(spawned_pid)

      assert_receive {:DOWN, ^ref, :process, ^spawned_pid, _reason}, 1000

      # Give the resource tracker a moment to process the DOWN message
      :timer.sleep(100)

      # Verify resources are automatically released after process death
      assert SimpleResourceTracker.available?(node, %{cpu: 2, memory: 1000})
    end

    test "handles multiple processes allocating resources" do
      node = Node.self()
      resources = %{cpu: 4, memory: 2000}

      # Register the node with resources
      SimpleResourceTracker.register(node, resources)

      test_pid = self()

      # Spawn two processes that allocate resources
      pid1 =
        spawn(fn ->
          :ok = SimpleResourceTracker.request(node, %{cpu: 1, memory: 500})
          send(test_pid, {:allocated, 1})
          # Keep process alive until explicitly killed
          receive do
            :shutdown -> :ok
          end
        end)

      pid2 =
        spawn(fn ->
          :ok = SimpleResourceTracker.request(node, %{cpu: 2, memory: 800})
          send(test_pid, {:allocated, 2})
          # Die sooner
          :timer.sleep(100)
        end)

      # Wait for both allocations
      receive do
        {:allocated, 1} -> :ok
      after
        1000 -> flunk("Process 1 allocation timeout")
      end

      receive do
        {:allocated, 2} -> :ok
      after
        1000 -> flunk("Process 2 allocation timeout")
      end

      # Verify only 1 CPU and 700 memory remain available
      assert SimpleResourceTracker.available?(node, %{cpu: 1, memory: 700})
      refute SimpleResourceTracker.available?(node, %{cpu: 2, memory: 800})

      # Wait for process 2 to die
      ref2 = Process.monitor(pid2)

      receive do
        {:DOWN, ^ref2, :process, ^pid2, _reason} -> :ok
      after
        1000 -> flunk("Process 2 did not die within timeout")
      end

      # Give resource tracker time to process
      :timer.sleep(100)

      # Verify process 2's resources are released but process 1's are still held
      assert SimpleResourceTracker.available?(node, %{cpu: 3, memory: 1500})
      refute SimpleResourceTracker.available?(node, %{cpu: 4, memory: 2000})

      # Shutdown process 1 and verify all resources are released
      send(pid1, :shutdown)
      :timer.sleep(100)

      assert SimpleResourceTracker.available?(node, %{cpu: 4, memory: 2000})
    end
  end

  describe "resource tracking" do
    test "registers node with capabilities" do
      # Register node with CPU and memory capabilities
      node = Node.self()
      caps = %{cpu: 4, memory: 8000}

      assert :ok = SimpleResourceTracker.register(node, caps)
      assert SimpleResourceTracker.available?(node, %{cpu: 2})
      assert SimpleResourceTracker.available?(node, %{memory: 4000})
    end

    test "checks resource availability" do
      node = Node.self()
      caps = %{cpu: 2, memory: 1000}

      SimpleResourceTracker.register(node, caps)

      # Check if resources are available
      assert SimpleResourceTracker.available?(node, %{cpu: 1})
      assert SimpleResourceTracker.available?(node, %{memory: 500})
      assert SimpleResourceTracker.available?(node, %{cpu: 1, memory: 500})

      # Check if unavailable resources are detected
      refute SimpleResourceTracker.available?(node, %{cpu: 3})
      refute SimpleResourceTracker.available?(node, %{memory: 1500})
      refute SimpleResourceTracker.available?(node, %{gpu: 1})
    end

    test "requests and releases resources" do
      node = Node.self()
      caps = %{cpu: 4, memory: 2000}

      SimpleResourceTracker.register(node, caps)

      # Request resources
      assert :ok = SimpleResourceTracker.request(node, %{cpu: 2})

      # Verify remaining resources
      assert SimpleResourceTracker.available?(node, %{cpu: 2})
      refute SimpleResourceTracker.available?(node, %{cpu: 3})

      # Request more resources
      assert :ok = SimpleResourceTracker.request(node, %{memory: 1500})

      # Verify remaining resources
      assert SimpleResourceTracker.available?(node, %{memory: 500})
      refute SimpleResourceTracker.available?(node, %{memory: 600})

      # Release resources
      assert :ok = SimpleResourceTracker.release(node, %{cpu: 1})
      assert SimpleResourceTracker.available?(node, %{cpu: 3})

      # Release all resources
      assert :ok = SimpleResourceTracker.release(node, %{cpu: 1, memory: 1500})
      assert SimpleResourceTracker.available?(node, %{cpu: 4, memory: 2000})
    end

    test "handles resource request failure" do
      node = Node.self()
      caps = %{cpu: 2}

      SimpleResourceTracker.register(node, caps)

      # Request more resources than available
      assert {:error, :resources_unavailable} = SimpleResourceTracker.request(node, %{cpu: 3})

      # Resource state unchanged
      assert SimpleResourceTracker.available?(node, %{cpu: 2})
    end
  end

  describe "allocate_and_claim/3" do
    test "spills to the next node after the first is saturated" do
      node_a = :claim_spill_a@localhost
      node_b = :claim_spill_b@localhost

      :ok = SimpleResourceTracker.register(Node.self(), %{compute: 0})
      :ok = SimpleResourceTracker.register(node_a, %{compute: 2})
      :ok = SimpleResourceTracker.register(node_b, %{compute: 2})

      nodes = [Node.self(), node_a, node_b]

      fun = fn id ->
        %Function{
          id: id,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [id],
          cost: %{compute: 1}
        }
      end

      assert {:ok, %{c1: ^node_a}} =
               SimpleResourceTracker.allocate_and_claim([fun.(:c1)], nodes)

      assert {:ok, %{c2: ^node_a}} =
               SimpleResourceTracker.allocate_and_claim([fun.(:c2)], nodes)

      assert {:ok, %{c3: ^node_b}} =
               SimpleResourceTracker.allocate_and_claim([fun.(:c3)], nodes)

      assert {:ok, %{c4: ^node_b}} =
               SimpleResourceTracker.allocate_and_claim([fun.(:c4)], nodes)
    end

    test "returns error when no node has remaining capacity" do
      node_a = :claim_full_a@localhost
      node_b = :claim_full_b@localhost

      :ok = SimpleResourceTracker.register(Node.self(), %{compute: 0})
      :ok = SimpleResourceTracker.register(node_a, %{compute: 1})
      :ok = SimpleResourceTracker.register(node_b, %{compute: 1})

      nodes = [Node.self(), node_a, node_b]
      fun = %Function{id: :x, args: [], code: &Elixir.Function.identity/1, cost: %{compute: 1}}

      assert {:ok, _} = SimpleResourceTracker.allocate_and_claim([%{fun | id: :a}], nodes)
      assert {:ok, _} = SimpleResourceTracker.allocate_and_claim([%{fun | id: :b}], nodes)

      assert {:error, :resources_unavailable} =
               SimpleResourceTracker.allocate_and_claim([%{fun | id: :c}], nodes)
    end

    test "splits a multi-function claim across nodes in one call" do
      node_a = :claim_batch_a@localhost
      node_b = :claim_batch_b@localhost

      :ok = SimpleResourceTracker.register(Node.self(), %{compute: 0})
      :ok = SimpleResourceTracker.register(node_a, %{compute: 2})
      :ok = SimpleResourceTracker.register(node_b, %{compute: 2})

      functions =
        for i <- 1..4 do
          %Function{
            id: :"batch_#{i}",
            args: [],
            code: &Elixir.Function.identity/1,
            cost: %{compute: 1}
          }
        end

      assert {:ok, allocations} =
               SimpleResourceTracker.allocate_and_claim(functions, [
                 Node.self(),
                 node_a,
                 node_b
               ])

      counts =
        allocations
        |> Map.values()
        |> Enum.frequencies()

      assert counts[node_a] == 2
      assert counts[node_b] == 2
    end

    test "releases claimed resources when the claimant process dies" do
      node_a = :claim_death_a@localhost

      :ok = SimpleResourceTracker.register(Node.self(), %{compute: 0})
      :ok = SimpleResourceTracker.register(node_a, %{compute: 1})

      nodes = [Node.self(), node_a]
      parent = self()

      pid =
        spawn(fn ->
          fun = %Function{
            id: :held,
            args: [],
            code: &Elixir.Function.identity/1,
            cost: %{compute: 1}
          }

          assert {:ok, %{held: ^node_a}} =
                   SimpleResourceTracker.allocate_and_claim([fun], nodes, self())

          send(parent, :claimed)

          receive do
            :exit_now -> :ok
          end
        end)

      assert_receive :claimed, 1_000
      refute SimpleResourceTracker.available?(node_a, %{compute: 1})

      ref = Process.monitor(pid)
      send(pid, :exit_now)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
      Process.sleep(50)

      assert SimpleResourceTracker.available?(node_a, %{compute: 1})
    end
  end

  describe "function allocation" do
    test "first_available allocation strategy", %{test_nodes: test_nodes} do
      # Get nodes for testing
      {node1, node2} = get_test_nodes(test_nodes)

      SimpleResourceTracker.register(node1, %{cpu: 2, memory: 1000})
      SimpleResourceTracker.register(node2, %{cpu: 4, memory: 2000})

      # Create functions with resource requirements
      functions = [
        %Function{id: :fn1, args: [], code: fn -> :ok end, cost: %{cpu: 1}},
        %Function{id: :fn2, args: [], code: fn -> :ok end, cost: %{cpu: 3}},
        %Function{id: :fn3, args: [], code: fn -> :ok end, cost: %{memory: 1500}}
      ]

      # Create capabilities map
      caps = %{
        node1 => %{cpu: 2, memory: 1000},
        node2 => %{cpu: 4, memory: 2000}
      }

      # Allocate functions
      allocations = SimpleAllocator.allocate(functions, caps)

      # First function should fit on node1
      assert Map.get(allocations, :fn1) == node1

      # Second function requires more CPU than node1 has, should go to node2
      assert Map.get(allocations, :fn2) == node2

      # Third function requires more memory than node1 has, should go to node2
      assert Map.get(allocations, :fn3) == node2
    end
  end

  # Helper functions

  # Get two test nodes - either from the distributed test nodes or use the local node twice
  defp get_test_nodes([]) do
    # No distributed nodes available, use local node
    local = Node.self()
    {local, local}
  end

  defp get_test_nodes([node | _rest]) do
    # Use local node and first distributed node
    {Node.self(), node}
  end
end
