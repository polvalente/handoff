# Handout

Handout is a library for building and executing Directed Acyclic Graphs (DAGs) of functions in Elixir.

## Features

- **Graph-based computation**: Define and execute complex computational graphs with dependencies.
- **Resource-aware scheduling**: Optimize computation based on available resources.
- **Distributed execution**: Run graph workloads across multiple nodes.
- **Fault tolerance**: Automatically handle node failures and task retries.

## Installation

The package can be installed by adding `handout` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:handout, "~> 0.1.0"}
  ]
end
```

## Usage

### Basic DAG Construction

Here's a simple example of how to create and validate a DAG:

```elixir
alias Handout.{DAG, Function}

# Create a new empty DAG
dag = Handout.new()

# Define a source function with no dependencies
source_fn = %Function{
  id: :source,
  args: [],
  code: fn -> 42 end
}

# Define a function that depends on the source function
transform_fn = %Function{
  id: :transform,
  args: [:source],
  code: fn results -> results[:source] * 2 end
}

# Define a sink function that depends on the transform function
sink_fn = %Function{
  id: :sink,
  args: [:transform],
  code: fn results -> "The answer is: #{results[:transform]}" end
}

# Add functions to the DAG
dag =
  dag
  |> DAG.add_function(source_fn)
  |> DAG.add_function(transform_fn)
  |> DAG.add_function(sink_fn)

# Validate the DAG to ensure it has no cycles and all dependencies exist
{:ok, valid_dag} = DAG.validate(dag)

# TODO: Execute the DAG (implementation coming soon)
```

### Distributed Execution (Coming Soon)

```elixir
# Define resource requirements for functions
cpu_intensive_fn = %Function{
  id: :cpu_heavy,
  args: [:source],
  code: fn results -> heavy_computation(results[:source]) end,
  cost: %{cpu: 4, memory: 2}  # Requires 4 CPU cores and 2GB memory
}

# Execute with resource-aware scheduling across nodes
# TODO: Execution API coming soon
```

## Documentation

Documentation will be available at [https://hexdocs.pm/handout](https://hexdocs.pm/handout) once published.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
