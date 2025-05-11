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

Handoff provides one built-in resource tracker:

1. `Handoff.SimpleResourceTracker`: Basic static resource tracking

## Allocation Strategies

Handoff's allocators decide which node should execute each function based on resource availability:

### Built-in Allocators

1. `Handoff.SimpleAllocator`: Uses first-available allocation strategy.
