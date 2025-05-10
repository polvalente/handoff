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
  code: fn -> heavy_computation() end,
  cost: %{cpu: 2, memory: 4000}
}

# Function requiring GPU resources
gpu_function = %Function{
  id: :gpu_task,
  args: [],
  code: fn -> gpu_computation() end,
  cost: %{gpu: 1, memory: 8000}
}

# Function with custom resource requirements
custom_function = %Function{
  id: :custom_task,
  args: [],
  code: fn -> special_computation() end,
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

Handoff provides one built-in resource tracker:

1. `Handoff.SimpleResourceTracker`: Basic static resource tracking

## Custom Resource Trackers

You can implement custom resource tracking by implementing the `Handoff.ResourceTracker` behavior:

```elixir
defmodule MyApp.CustomResourceTracker do
  @behaviour Handoff.ResourceTracker

  @impl true
  def register(node, caps) do
    # Custom registration logic
    :ok
  end

  @impl true
  def available?(node, req) do
    # Custom availability checking logic
    true
  end

  @impl true
  def request(node, req) do
    # Custom resource reservation logic
    :ok
  end
end
```

## Allocation Strategies

Handoff's allocators decide which node should execute each function based on resource availability:

### Built-in Allocators

1. `Handoff.SimpleAllocator`: Uses first-available or load-balanced strategies
2. `Handoff.CostOptimizedAllocator`: Optimizes allocation to minimize overall resource usage

```elixir
# Execute with the cost-optimized allocator
{:ok, results} = Handoff.execute_distributed(dag,
  allocation_strategy: :cost_optimized
)
```

### Custom Allocators

You can implement custom allocation strategies by implementing the `Handoff.Allocator` behavior:

```elixir
defmodule MyApp.CustomAllocator do
  @behaviour Handoff.Allocator

  @impl true
  def allocate(functions, capabilities) do
    # Custom allocation logic
    # Return a map of function IDs to node names
    %{}
  end
end

# Use the custom allocator
Handoff.start(allocator: MyApp.CustomAllocator)
```

## Resource Monitoring

Handoff provides telemetry events for monitoring resource usage:

```elixir
# Attach to resource allocation events
:telemetry.attach(
  "resource-monitoring",
  [:handoff, :resource, :allocation],
  fn _name, measurements, metadata, _config ->
    function_id = metadata.function_id
    node = metadata.node
    allocated = measurements.allocated

    IO.puts("Function #{inspect(function_id)} allocated #{inspect(allocated)} on node #{node}")
  end,
  nil
)
```

## Resource Visualization

Handoff's visualization tools can help you understand resource usage:

```elixir
# Generate resource utilization visualization data
{:ok, utilization_data} = Handoff.Visualization.resource_utilization(results)
```

## Best Practices

1. **Right-size your resource definitions**: Be accurate but not too restrictive with resource requirements
2. **Consider function locality**: Place interdependent functions on the same node to reduce data transfer
3. **Monitor resource usage**: Use telemetry events to track actual resource consumption
4. **Update dynamically**: Adjust resource availability at runtime for better utilization
5. **Balance load**: Use appropriate allocation strategies to distribute work evenly
