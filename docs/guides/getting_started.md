# Getting Started with Handout

This guide will help you get started with Handout, a library for building and executing Directed Acyclic Graphs (DAGs) of functions in Elixir.

## Installation

Add `handout` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:handout, "~> 0.1.0"}
  ]
end
```

Then run:

```bash
mix deps.get
```

## Basic Usage

### Starting Handout

Before using Handout, you need to start its supervision tree:

```elixir
# Start Handout with default options
Handout.start()

# Or with custom options
Handout.start(allocation_strategy: :load_balanced)
```

### Creating a Simple DAG

Let's create a simple computation graph:

```elixir
alias Handout.Function

# Create a new DAG
dag = Handout.new()

# Define a source function with no dependencies
source_fn = %Function{
  id: :data_source,
  args: [],
  code: fn -> [1, 2, 3, 4, 5] end
}

# Define a transformation function
transform_fn = %Function{
  id: :double,
  args: [:data_source],
  code: fn %{data_source: data} -> Enum.map(data, &(&1 * 2)) end
}

# Define a final aggregation function
aggregate_fn = %Function{
  id: :sum,
  args: [:double],
  code: fn %{double: data} -> Enum.sum(data) end
}

# Add functions to the DAG
dag =
  dag
  |> Handout.DAG.add_function(source_fn)
  |> Handout.DAG.add_function(transform_fn)
  |> Handout.DAG.add_function(aggregate_fn)

# Validate the DAG
{:ok, valid_dag} = Handout.DAG.validate(dag)
```

### Executing the DAG

```elixir
# Execute the DAG
{:ok, results} = Handout.execute(valid_dag)

# Access the final result
sum_result = results[:sum]
IO.puts("The sum is: #{sum_result}")  # Output: The sum is: 30
```

## Using Extra Arguments

You can provide additional arguments to functions at execution time:

```elixir
# Define a function that uses extra arguments
parametrized_fn = %Function{
  id: :parametrized,
  args: [:data_source],
  code: fn %{data_source: data}, multiplier ->
    Enum.map(data, &(&1 * multiplier))
  end,
  extra_args: [3]  # Pass 3 as the multiplier
}
```

## Error Handling

Handout provides proper error handling during validation and execution:

```elixir
# DAG with missing dependencies
invalid_dag =
  Handout.new()
  |> Handout.DAG.add_function(%Function{
    id: :invalid,
    args: [:non_existent],  # This dependency doesn't exist
    code: fn _ -> :error end
  })

case Handout.DAG.validate(invalid_dag) do
  {:ok, _} ->
    IO.puts("DAG is valid")

  {:error, {:missing_dependencies, missing}} ->
    IO.puts("DAG has missing dependencies: #{inspect(missing)}")
end
```

## Next Steps

Now that you understand the basics of Handout, you might want to explore:

- [Distributed Execution](distributed_execution.md) - Execute your DAGs across multiple nodes
- [Resource Management](resource_management.md) - Define and manage computational resources
- [Advanced Allocation Strategies](allocation_strategies.md) - Optimize function allocation to nodes
