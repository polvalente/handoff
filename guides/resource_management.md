# Resource Management

This guide explains how to define, track, and allocate computational resources with Handoff.

## Resource Types

Handoff allows you to define any type of computational resource. Common resource types include:

- `cpu` - CPU cores or processing units
- `memory` - Memory in MB
- `gpu` - GPU units
- Custom resources (e.g., `network_bandwidth`, `storage`, `specialized_hardware`)

## Defining Resource Requirements

Resource requirements are defined at the function level using the `:cost` field:

```elixir
alias Handoff.Function

# Function requiring 2 CPU cores and 4GB memory
cpu_function = %Function{
  id: :cpu_task,
  args: [],
  code: &SomeModule.heavy_computation/0,
  cost: %{cpu: 2, memory: 4000}
}

# Function requiring GPU resources
gpu_function = %Function{
  id: :gpu_task,
  args: [],
  code: &SomeModule.gpu_computation/0,
  cost: %{gpu: 1, memory: 8000}
}

# Function with custom resource requirements
custom_function = %Function{
  id: :custom_task,
  args: [],
  code: &SomeModule.special_computation/0,,
  cost: %{specialized_hardware: 1, memory: 2000}
}
```

## Node Capabilities

Each node in a Handoff cluster advertises its available resources:

```elixir
# Register node capabilities
Handoff.register_node(Node.self(), %{
  cpu: 8,          # 8 CPU cores
  memory: 16000,   # 16GB memory
  gpu: 2,          # 2 GPU units
  specialized_hardware: 1  # Custom resource
})
```

## Resource Tracking

Handoff's resource trackers monitor resource availability across nodes:

```elixir
# Check if a node has sufficient resources
has_resources = Handoff.resources_available?(
  :"node1@example.com",
  %{cpu: 4, memory: 8000}
)

if has_resources do
  IO.puts("Node has sufficient resources")
else
  IO.puts("Node lacks required resources")
end
```

### Built-in Resource Trackers

A **resource tracker** in Handoff is responsible for monitoring and managing the computational resources available on each node in the cluster. It keeps track of what resources are registered, what is currently in use, and what is available for new tasks. Resource trackers are essential for ensuring that functions are only scheduled on nodes with sufficient available resources.

#### `Handoff.SimpleResourceTracker`

Handoff provides a built-in resource tracker: `Handoff.SimpleResourceTracker`.

- **Type:** Static, in-memory, per-node resource tracker
- **Implementation:** Uses an ETS table and GenServer to track resources for each node
- **Scope:** Tracks resources registered on the local node; does not persist state or synchronize across nodes
- **API:**
  - `register(node, caps)`: Register a node and its resource capabilities (e.g., `%{cpu: 4, memory: 8000}`)
  - `available?(node, req)`: Check if a node has enough available resources for a given requirement
  - `request(node, req)`: Reserve resources for a task (returns `:ok` or `{:error, :resources_unavailable}`)
  - `release(node, req)`: Release previously reserved resources
  - `get_capabilities()`: Get the full resource capabilities of the local node

**Example usage:**

```elixir
alias Handoff.SimpleResourceTracker

# Register a node with its capabilities
SimpleResourceTracker.register(Node.self(), %{cpu: 4, memory: 8000})

# Check if resources are available
if SimpleResourceTracker.available?(Node.self(), %{cpu: 2, memory: 2000}) do
  # Request resources for a task
  case SimpleResourceTracker.request(Node.self(), %{cpu: 2, memory: 2000}) do
    :ok ->
      # ... run your task ...
      # Release resources when done
      SimpleResourceTracker.release(Node.self(), %{cpu: 2, memory: 2000})
    {:error, :resources_unavailable} ->
      IO.puts("Not enough resources available!")
  end
else
  IO.puts("Resources not available")
end
```

**How it works:**
- When a node is registered, its total resource capacity is stored.
- When resources are requested, the tracker checks if enough are available (total minus currently used). If so, it marks them as used.
- When resources are released, the available amount increases again.
- All tracking is local to the node running the tracker.

**Limitations:**
- `SimpleResourceTracker` is static and in-memory only. If the node restarts, resource state is lost.
- It does not synchronize state between nodes; each node tracks its own resources independently.
- Not suitable for dynamic or persistent resource tracking across a distributed cluster.

**Extending Resource Tracking:**
If you need more advanced tracking (e.g., persistent, distributed, or dynamic resource management), you can implement the `Handoff.ResourceTracker` behaviour and provide your own tracker module.

See `lib/handoff/resource_tracker.ex` for the required callbacks and documentation.

## Allocation Strategies

Handoff's allocators decide which node should execute each function based on resource availability:

### Built-in Allocators

1. `Handoff.SimpleAllocator`: Uses first-available allocation strategy.

#### Example: Allocating Multiple Functions to Nodes

Suppose you have a cluster with two nodes, each with different resource capacities:

```elixir
nodes_caps = %{
  :node1@host => %{cpu: 4, memory: 8000},
  :node2@host => %{cpu: 2, memory: 4000}
}

functions = [
  %Handoff.Function{id: :f1, cost: %{cpu: 2, memory: 2000}},
  %Handoff.Function{id: :f2, cost: %{cpu: 1, memory: 1000}},
  %Handoff.Function{id: :f3, cost: %{cpu: 3, memory: 4000}},
  %Handoff.Function{id: :f4, cost: %{cpu: 1, memory: 1000}}
]

assignments = Handoff.SimpleAllocator.allocate(functions, nodes_caps)
IO.inspect(assignments)
# Example output:
# %{f1: :node1@host, f2: :node1@host, f3: :node2@host, f4: :node2@host}
```

**Explanation:**
- The allocator tries to assign each function to the first node with enough available resources.
- After each assignment, the node's available resources are reduced for subsequent allocations.
- If a node cannot satisfy a function's requirements, the allocator tries the next node.

#### Example: Handling Insufficient Resources

If no node has enough resources for a function, the allocator will still assign it (to the first node), but you should handle this case in your workflow:

```elixir
nodes_caps = %{
  :node1@host => %{cpu: 1, memory: 1000},
  :node2@host => %{cpu: 1, memory: 1000}
}

functions = [
  %Handoff.Function{id: :big_task, cost: %{cpu: 2, memory: 2000}}
]

assignments = Handoff.SimpleAllocator.allocate(functions, nodes_caps)
IO.inspect(assignments)
# Output: %{big_task: :node1@host}
# Note: In this case, neither node actually has enough resources. You should check resource availability before running the task.
```

#### Example: Pinned and Collocated Functions

You can pin a function to a specific node, or collocate it with another function:

```elixir
functions = [
  %Handoff.Function{id: :f1, cost: %{cpu: 1}, node: :node2@host}, # pinned to node2
  %Handoff.Function{id: :f2, cost: %{cpu: 1}},                    # dynamic
  %Handoff.Function{id: :f3, cost: %{cpu: 1}, node: {:collocated, :f2}} # must run on same node as f2
]

nodes_caps = %{
  :node1@host => %{cpu: 2},
  :node2@host => %{cpu: 2}
}

assignments = Handoff.SimpleAllocator.allocate(functions, nodes_caps)
IO.inspect(assignments)
# Example output:
# %{f1: :node2@host, f2: :node1@host, f3: :node1@host}
# f1 is pinned to node2, f2 is dynamically assigned, f3 is collocated with f2
```

**Tips:**
- Always check resource availability before running tasks, especially if using custom allocators or dynamic resource changes.
- For advanced allocation strategies, you can implement your own allocator module using the `Handoff.Allocator` behaviour.
