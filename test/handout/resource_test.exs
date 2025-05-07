defmodule Handoff.ResourceTest do
  use ExUnit.Case, async: false

  alias Handoff.Function
  alias Handoff.SimpleResourceTracker
  alias Handoff.SimpleAllocator

  setup do
    # Get test nodes if available
    test_nodes = Application.get_env(:handoff, :test_nodes, [])

    # Register code paths on remote nodes for module visibility
    for node <- test_nodes do
      :erpc.call(node, :code, :add_paths, [:code.get_path()])
    end

    {:ok, %{test_nodes: test_nodes}}
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
      allocations = SimpleAllocator.allocate(functions, caps, :first_available)

      # First function should fit on node1
      assert Map.get(allocations, :fn1) == node1

      # Second function requires more CPU than node1 has, should go to node2
      assert Map.get(allocations, :fn2) == node2

      # Third function requires more memory than node1 has, should go to node2
      assert Map.get(allocations, :fn3) == node2
    end

    test "load_balanced allocation strategy", %{test_nodes: test_nodes} do
      # Get nodes for testing
      {node1, node2} = get_test_nodes(test_nodes)

      SimpleResourceTracker.register(node1, %{cpu: 4, memory: 2000})
      SimpleResourceTracker.register(node2, %{cpu: 4, memory: 2000})

      # Create functions with resource requirements
      functions = [
        %Function{id: :fn1, args: [], code: fn -> :ok end, cost: %{cpu: 2}},
        %Function{id: :fn2, args: [], code: fn -> :ok end, cost: %{cpu: 2}},
        %Function{id: :fn3, args: [], code: fn -> :ok end, cost: %{cpu: 1}},
        %Function{id: :fn4, args: [], code: fn -> :ok end, cost: %{cpu: 1}}
      ]

      # Create capabilities map
      caps = %{
        node1 => %{cpu: 4, memory: 2000},
        node2 => %{cpu: 4, memory: 2000}
      }

      # Allocate functions
      allocations = SimpleAllocator.allocate(functions, caps, :load_balanced)

      # All functions should be allocated
      assert map_size(allocations) == 4

      # If we have distributed nodes, check balanced allocation
      if node1 != node2 do
        # Count allocations per node
        node1_count = Enum.count(allocations, fn {_, node} -> node == node1 end)
        node2_count = Enum.count(allocations, fn {_, node} -> node == node2 end)

        # Should have balanced allocation (2 functions per node)
        assert node1_count == 2
        assert node2_count == 2
      end
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
