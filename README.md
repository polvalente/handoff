# Handoff

Handoff is a library for building and executing Directed Acyclic Graphs (DAGs) of functions in Elixir.

## Features

- **Graph-based computation**: Define and execute complex computational graphs with dependencies.
- **Resource-aware scheduling**: Optimize computation based on available resources.
- **Distributed execution**: Run graph workloads across multiple nodes.
- **Fault tolerance**: Task retries on failure up to a configured maximum number of retries.

## Installation

The package can be installed by adding `handoff` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:handoff, "~> 0.1.0"}
  ]
end
```

## Usage

### Basic DAG Construction

Here's a simple example of how to create and validate a DAG.
We'll assume we have a module `MyTasks` with a few helper functions:

```elixir
defmodule MyTasks do
  def format_output(value), do: "The final answer is: #{value}"
end
```

Now, let's build the DAG:

```elixir
alias Handoff.{DAG, Function}

# Create a new empty DAG
dag = DAG.new() # Handoff.DAG.new() is also valid

# Define a source function with no dependencies
source_fn = %Function{
  id: :source,
  args: [],
  code: &Elixir.Function.identity/1,
  extra_args: [42]
}

# Define a function that depends on the source function
transform_fn = %Function{
  id: :transform,
  args: [:source], # Result of :source is passed to &*/2
  code: &*/2,
  extra_args: [2]
}

# Define a sink function that depends on the transform function
sink_fn = %Function{
  id: :sink,
  args: [:transform], # Result of :transform is passed to MyTasks.format_output/1
  code: &MyTasks.format_output/1
}

# Add functions to the DAG
dag =
  dag
  |> DAG.add_function(source_fn)
  |> DAG.add_function(transform_fn)
  |> DAG.add_function(sink_fn)

# Validate the DAG to ensure it has no cycles and all dependencies exist
case DAG.validate(dag) do
  :ok ->
    IO.puts("DAG is valid!")
  {:error, reason} ->
    IO.puts("DAG is invalid: #{inspect(reason)}")
end
```

### Executing a DAG

Once a DAG is constructed and validated, you can execute it. Handoff's `DistributedExecutor` can run DAGs locally or across multiple nodes. For local execution:

```elixir
# (Assuming dag from the previous example is validated and :ok)

# Ensure Handoff application is started (typically in your application.ex)
# For scripts or Livebooks, you might need:
# {:ok, _pid} = Handoff.start_link() # Or Handoff.Application.start(:normal, [])
# Handoff.DistributedExecutor.register_local_node(%{cpu: 2, memory: 1024}) # Example capabilities

case Handoff.DistributedExecutor.execute(dag) do
  {:ok, results_map} ->
    # results_map is %{dag_id: ..., results: %{source: 42, transform: 84, sink: "The final answer is: 84"}, allocations: %{...}}
    final_message = results_map.results[:sink]
    IO.puts(final_message)
    # => "The final answer is: 84"

  {:error, reason} ->
    IO.puts("Error executing DAG: #{inspect(reason)}")
end
```

Note: The `Handoff.DistributedExecutor.execute/2` function returns a map containing the `:dag_id`, the execution `:results` (a map of function ID to its output), and `:allocations` (a map of function ID to the node it ran on).

<!-- TODO: Add note about starting Handoff application if not already running -->

### Distributed Execution

Handoff is designed to distribute DAG execution across multiple Erlang nodes. To enable this, you need to:

1. Ensure nodes are connected in an Erlang cluster.
2. Start the `Handoff` application on each node.
3. Register each node's capabilities (e.g., CPU, memory) with `Handoff.DistributedExecutor.register_local_node/1`.
4. Optionally, specify node placement preferences or resource costs for your functions.

Here's a conceptual example. We'll reuse `MyTasks.format_output/1` and imagine a `MyDistributedTasks` module:

```elixir
# On all participating nodes:
# Ensure Handoff application is started (e.g., in application.ex or manually)
# Handoff.Application.start(:normal, []) # Or Handoff.start_link()

# Register node capabilities (example for one node)
# This would typically be done on each node with its specific resources.
node_capabilities = %{cpu: 4, memory: 8192, gpu: 1}
Handoff.DistributedExecutor.register_local_node(node_capabilities)

# Example Task Module (must be available on all nodes)
defmodule MyDistributedTasks do
  def load_data(path), do: "Loaded: #{path}" # Simulate data loading
  def process_on_gpu(data), do: "Processed [#{data}] on GPU" # Simulate GPU work
end
```

Then, on the orchestrating node, define and execute the DAG:

```elixir
alias Handoff.{DAG, Function}

dag = DAG.new("distributed_example_dag")

# Function that might run on any node based on availability
load_fn = %Function{
  id: :load_data,
  args: [],
  code: &MyDistributedTasks.load_data/1,
  extra_args: ["/path/to/shared/data.txt"],
  cost: %{cpu: 1, memory: 1000} # Basic cost
}

# Function that explicitly requests GPU resources
gpu_fn = %Function{
  id: :gpu_process,
  args: [:load_data],
  code: &MyDistributedTasks.process_on_gpu/1,
  cost: %{cpu: 2, memory: 2048, gpu: 1} # Requires a GPU
}

# Function to format the output, can run anywhere
format_fn = %Function{
  id: :format_result,
  args: [:gpu_process],
  code: &MyTasks.format_output/1, # Reusing from previous example
  cost: %{cpu: 1, memory: 500}
}

distributed_dag =
  dag
  |> DAG.add_function(load_fn)
  |> DAG.add_function(gpu_fn)
  |> DAG.add_function(format_fn)

# Validate
:ok = DAG.validate(distributed_dag)

# Execute the DAG - Handoff will attempt to distribute based on costs and availability
case Handoff.DistributedExecutor.execute(distributed_dag) do
  {:ok, results_map} ->
    IO.puts("Distributed DAG completed!")
    IO.puts("Final result: #{results_map.results[:format_result]}")
    IO.inspect(results_map.allocations, label: "Function Allocations")
  {:error, reason} ->
    IO.puts("Distributed DAG execution failed: #{inspect(reason)}")
end
```

By default, `Handoff.DistributedExecutor` will try to allocate functions to nodes that satisfy their resource `:cost` and are available. If all functions can be satisfied by the local node and no other nodes are registered or available, execution will be local-only. You can also specify `node: :some_node_name` in the `Handoff.Function` struct to pin a function to a particular node, or use more advanced allocation strategies via options in `execute/2`.

## Interactive Examples

We provide interactive [Livebook](https://livebook.dev/) examples to help you get started:

- **Simple Pipeline** - A basic data processing pipeline showing core concepts
- **Distributed Image Processing** - A complex example with resource-aware distributed execution

Check out the [`livebooks/`](livebooks/) directory for these interactive notebooks.

## Running Tests

To run the test suite, use the following command:

```bash
mix test
```

## Documentation

Official documentation is available at [https://hexdocs.pm/handoff](https://hexdocs.pm/handoff)

### Building Documentation Locally

This project uses [ExDoc](https://github.com/elixir-lang/ex_doc) to generate documentation. To build the documentation locally, run:

```bash
mix docs
```

This will generate documentation in the `doc/` directory. You can open `doc/index.html` in your browser to view it.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details.
