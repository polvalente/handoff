# Distributed Execution

This guide explains how to execute Handout DAGs across multiple Elixir nodes for improved performance and scalability.

## Prerequisites

- A working Elixir cluster with connected nodes
- Handout installed on all nodes
- Basic understanding of Handout DAGs (see [Getting Started](getting_started.md))

## Setting Up Nodes

Before executing a DAG in a distributed environment, you need to set up your nodes:

```elixir
# On each node, start Handout
Handout.start()

# Register the local node with its capabilities
Handout.register_local_node(%{
  cpu: 8,        # 8 CPU cores
  memory: 16000, # 16GB memory
  gpu: 1         # 1 GPU unit
})
```

### Node Discovery

Handout provides automatic node discovery within a connected Erlang cluster:

```elixir
# Discover all nodes in the cluster and register their capabilities
{:ok, discovered_nodes} = Handout.discover_nodes()

# See what nodes are available and their resources
IO.inspect(discovered_nodes)
# Example output:
# %{
#   :"node1@192.168.1.100" => %{cpu: 8, memory: 16000, gpu: 1},
#   :"node2@192.168.1.101" => %{cpu: 4, memory: 8000},
#   :"node3@192.168.1.102" => %{cpu: 16, memory: 32000, gpu: 2}
# }
```

## Resource-Aware Functions

To take advantage of distributed execution, define functions with resource requirements:

```elixir
alias Handout.Function

# CPU-intensive function
cpu_heavy_fn = %Function{
  id: :cpu_intensive,
  args: [:input_data],
  code: fn %{input_data: data} -> heavy_computation(data) end,
  cost: %{cpu: 4, memory: 2000}  # Requires 4 CPU cores, 2GB memory
}

# GPU-accelerated function
gpu_fn = %Function{
  id: :gpu_task,
  args: [:preprocessed_data],
  code: fn %{preprocessed_data: data} -> gpu_computation(data) end,
  cost: %{gpu: 1, memory: 4000}  # Requires 1 GPU, 4GB memory
}
```

## Distributed Execution

Execute your DAG across the cluster using `execute_distributed`:

```elixir
# Build and validate your DAG
dag =
  Handout.new()
  |> Handout.DAG.add_function(source_fn)
  |> Handout.DAG.add_function(cpu_heavy_fn)
  |> Handout.DAG.add_function(gpu_fn)
  |> Handout.DAG.add_function(aggregation_fn)

{:ok, valid_dag} = Handout.DAG.validate(dag)

# Execute the DAG across the cluster
{:ok, results} = Handout.execute_distributed(valid_dag,
  allocation_strategy: :load_balanced,
  max_retries: 3
)
```

## Allocation Strategies

Handout provides several built-in allocation strategies:

1. `:first_available` - Assigns functions to the first node with sufficient resources
2. `:load_balanced` - Distributes functions evenly across nodes based on load
3. `:cost_optimized` - Optimizes allocation to minimize overall resource usage

```elixir
# Using the cost-optimized allocator
{:ok, results} = Handout.execute_distributed(valid_dag,
  allocation_strategy: :cost_optimized
)
```

## Fault Tolerance

Distributed execution in Handout includes automatic fault tolerance:

- Task retry on failure (configurable with `max_retries`)
- Node failure detection
- Result synchronization across nodes

If a node fails during execution, its tasks will be reassigned to other suitable nodes.

## Monitoring Distributed Execution

Handout provides telemetry events for monitoring distributed execution:

```elixir
# Set up a handler for telemetry events
:telemetry.attach(
  "handout-monitoring",
  [:handout, :execution, :function, :start],
  fn name, measurements, metadata, _config ->
    IO.puts("Function #{metadata.function_id} started on node #{metadata.node}")
    IO.puts("Measurements: #{inspect(measurements)}")
  end,
  nil
)
```

## Advanced Configuration

For more control over distributed execution:

```elixir
# Custom resource tracker
Handout.start(
  resource_tracker: MyApp.CustomResourceTracker,
  result_store: MyApp.CustomResultStore
)

# Dynamic resource updates
Handout.update_node_resources(Node.self(), %{cpu: 12, memory: 24000})
```

## Visualizing Distributed Execution

Handout includes tools to visualize the distribution of tasks:

```elixir
# Generate a visualization of the execution graph with node assignments
{:ok, graph_data} = Handout.Visualization.execution_graph(results)
```

The resulting graph data can be used with visualization libraries to create an interactive view of your distributed execution.
