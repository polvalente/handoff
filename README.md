# Handout

A general-purpose library for executing directed acyclic graphs (DAGs) of functions across distributed Erlang nodes.

## Overview

Handout provides a framework for defining function dependencies, resource requirements, and allocation strategies to optimize execution based on available resources. It enables you to:

- Create flexible systems for distributing computational workloads across BEAM nodes
- Support custom resource tracking and allocation strategies
- Provide optimized execution of interdependent functions
- Maintain independence from specific application domains while supporting specialization

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `handout` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:handout, "~> 0.1.0"}
  ]
end
```

## Usage

### Basic Example

```elixir
# Define functions with their dependencies
functions = [
  Handout.function(:load_data, &MyModule.load_data/1),
  Handout.function(:process_data, &MyModule.process_data/1, args: [{:load_data, 0}]),
  Handout.function(:output_results, &MyModule.output_results/1, args: [{:process_data, 0}])
]

# Execute the graph
{:ok, executor} = Handout.execute(functions)

# Wait for all functions to complete
results = Handout.await_completion(executor)

# Access specific results
final_output = Handout.get_result(executor, :output_results)
```

### Resource-Aware Execution

```elixir
# Define functions with costs
functions = [
  Handout.function(:load_data, &MyModule.load_data/1, 
    cost: %{memory: 2000, cpu_intensive: false}),
  Handout.function(:process_data, &MyModule.process_data/1, 
    args: [{:load_data, 0}], 
    cost: %{memory: 500, gpu_memory: 1000, cpu_intensive: true}),
  Handout.function(:output_results, &MyModule.output_results/1, 
    args: [{:process_data, 0}], 
    cost: %{memory: 200, cpu_intensive: false})
]

# Define resource specifications
resources = [
  %{
    node: :"node1@example.com",
    capabilities: %{memory: 8000, gpu_memory: 0, has_cuda: false}
  },
  %{
    node: :"node2@example.com",
    capabilities: %{memory: 4000, gpu_memory: 4000, has_cuda: true}
  }
]

# Define allocation strategy (optional)
allocator = MyCustomAllocator

# Execute the graph
{:ok, executor} = Handout.execute(functions, resources: resources, allocator: allocator)
```

## Documentation

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc):

```
mix docs
```

Once published, the docs can be found at <https://hexdocs.pm/handout>.

## Features

- **Function Dependency Resolution**: Automatically resolve and manage dependencies between functions
- **Resource Allocation**: Allocate functions to nodes based on resource requirements and availability
- **Distribution**: Execute functions across distributed Erlang nodes
- **Customizable Allocation Strategies**: Implement custom allocation strategies
- **Fault Tolerance**: Handle node and function execution failures

## Status

This project is currently in active development. See the PROJECT_MANIFEST.md file for the full implementation roadmap.

