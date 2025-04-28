defmodule Handout.Benchmarking do
  @moduledoc """
  Benchmarking utilities for measuring and optimizing Handout performance.

  This module provides tools for:
  - Measuring DAG execution times
  - Comparing different allocator strategies
  - Profiling resource usage patterns
  - Generating performance reports

  ## Usage

  ```elixir
  # Create a test DAG
  dag = Handout.Benchmarking.generate_test_dag(100)

  # Benchmark execution with different allocators
  results = Handout.Benchmarking.compare_allocators(dag, [
    Handout.SimpleAllocator,
    Handout.CostOptimizedAllocator
  ])

  # Generate a performance report
  report = Handout.Benchmarking.generate_report(results)
  ```
  """

  alias Handout.Function
  alias Handout.Logger, as: HandoutLogger

  @doc """
  Generate a test DAG with specified size and complexity.

  ## Parameters
  - `size`: Number of functions in the DAG
  - `options`: Options for controlling DAG characteristics
    - `:max_depth` - Maximum DAG depth (default: 5)
    - `:max_width` - Maximum width at any level (default: 10)
    - `:max_deps` - Maximum dependencies per function (default: 3)
    - `:cost_range` - Range for random costs (default: {1, 10})
    - `:resource_types` - List of resource types to use (default: [:cpu, :memory, :gpu])

  ## Returns
  - A list of Function structs representing the DAG
  """
  def generate_test_dag(size, options \\ []) do
    # Set default options
    options =
      Keyword.merge(
        [
          max_depth: 5,
          max_width: 10,
          max_deps: 3,
          cost_range: {1, 10},
          resource_types: [:cpu, :memory, :gpu]
        ],
        options
      )

    HandoutLogger.info("Generating test DAG", size: size)

    # Create functions in layers (by depth)
    {functions, _} =
      create_layered_dag(
        size,
        options[:max_depth],
        options[:max_width],
        options[:max_deps],
        options[:cost_range],
        options[:resource_types]
      )

    HandoutLogger.info("Test DAG generated", function_count: length(functions))

    functions
  end

  @doc """
  Compare the performance of different allocator implementations.

  ## Parameters
  - `dag`: The DAG to use for testing
  - `allocators`: List of allocator modules to compare
  - `caps`: Node capabilities map to use (will generate if not provided)
  - `iterations`: Number of test iterations (default: 3)

  ## Returns
  - Map of performance metrics for each allocator
  """
  def compare_allocators(dag, allocators, caps \\ nil, iterations \\ 3) do
    # Generate capabilities if not provided
    caps = caps || generate_test_capabilities(length(dag))

    HandoutLogger.info("Comparing allocators",
      allocator_count: length(allocators),
      function_count: length(dag),
      iterations: iterations
    )

    # For each allocator
    Enum.map(allocators, fn allocator ->
      # Run benchmark iterations
      iteration_results =
        Enum.map(1..iterations, fn i ->
          HandoutLogger.debug("Running benchmark iteration",
            allocator: allocator,
            iteration: i
          )

          benchmark_allocator(allocator, dag, caps)
        end)

      # Aggregate results
      aggregated = aggregate_results(iteration_results)

      {allocator, aggregated}
    end)
    |> Map.new()
  end

  @doc """
  Measure the execution time of a DAG.

  ## Parameters
  - `dag`: The DAG to execute
  - `options`: Execution options
    - `:allocator` - Allocator module to use (default: Handout.SimpleAllocator)

  ## Returns
  - Map of execution metrics
  """
  def benchmark_execution(dag, options \\ []) do
    # Set default options
    options =
      Keyword.merge(
        [
          allocator: Handout.SimpleAllocator
        ],
        options
      )

    HandoutLogger.info("Benchmarking DAG execution",
      function_count: length(dag),
      allocator: options[:allocator]
    )

    # Start timing
    start_time = System.monotonic_time(:millisecond)

    # Execute the DAG
    result = Handout.execute(dag, options)

    # End timing
    end_time = System.monotonic_time(:millisecond)
    execution_time = end_time - start_time

    # Calculate metrics
    metrics = %{
      total_time: execution_time,
      success_rate: calculate_success_rate(result),
      throughput: length(dag) / (execution_time / 1000)
    }

    HandoutLogger.info("DAG execution benchmarked",
      total_time: metrics.total_time,
      success_rate: metrics.success_rate,
      throughput: metrics.throughput
    )

    metrics
  end

  @doc """
  Generate a benchmark report comparing different configurations.

  ## Parameters
  - `results`: Map of benchmark results by allocator/configuration

  ## Returns
  - String containing the report
  """
  def generate_report(results) do
    HandoutLogger.info("Generating benchmark report", configuration_count: map_size(results))

    # Create a formatted report
    report = """
    # Handout Benchmark Report

    ## Summary

    | Configuration | Total Time (ms) | Success Rate | Throughput (fn/s) |
    |---------------|----------------|--------------|-------------------|
    #{format_summary_table(results)}

    ## Detailed Results

    #{format_detailed_results(results)}
    """

    HandoutLogger.info("Benchmark report generated", size: byte_size(report))

    report
  end

  # Helper functions

  # Create a layered DAG with specified parameters
  defp create_layered_dag(size, max_depth, max_width, max_deps, cost_range, resource_types) do
    # Distribute functions across layers
    {functions_per_layer, remainder} = div_rem(size, max_depth)
    functions_per_layer = min(functions_per_layer, max_width)

    # Create initial distribution, ensuring each layer has at least one function
    # and doesn't exceed max_width
    distributed_counts =
      Enum.map(1..max_depth, fn i ->
        extra = if i <= remainder, do: 1, else: 0
        min(functions_per_layer + extra, max_width)
      end)

    # Ensure we don't exceed the requested size
    distributed_size = Enum.sum(distributed_counts)
    actual_size = min(size, distributed_size)

    # Adjust if we distributed too many
    distributed_counts =
      if distributed_size > actual_size do
        {distributed_counts, _} =
          Enum.reduce(max_depth..1, {distributed_counts, distributed_size - actual_size}, fn i,
                                                                                             {counts,
                                                                                              remaining} ->
            if remaining > 0 do
              current = Enum.at(counts, i - 1)
              reduction = min(current - 1, remaining)

              counts = List.update_at(counts, i - 1, fn _ -> current - reduction end)
              remaining = remaining - reduction

              {counts, remaining}
            else
              {counts, 0}
            end
          end)

        distributed_counts
      else
        distributed_counts
      end

    # Generate functions layer by layer
    {functions, id_counter} =
      Enum.reduce(Enum.with_index(distributed_counts), {[], 1}, fn {count, layer_idx},
                                                                   {all_functions, id_counter} ->
        layer_functions =
          if layer_idx == 0 do
            # First layer has no dependencies
            create_layer_functions(count, [], id_counter, max_deps, cost_range, resource_types)
          else
            # Get dependency candidates from previous layers
            deps =
              Enum.take(
                all_functions,
                length(all_functions) - Enum.at(distributed_counts, layer_idx - 1)
              )
              |> Enum.map(& &1.id)

            create_layer_functions(count, deps, id_counter, max_deps, cost_range, resource_types)
          end

        {all_functions ++ elem(layer_functions, 0), id_counter + count}
      end)

    {functions, id_counter}
  end

  # Create a layer of functions with dependencies on previous layers
  defp create_layer_functions(
         count,
         dep_candidates,
         id_start,
         max_deps,
         {min_cost, max_cost},
         resource_types
       ) do
    functions =
      Enum.map(0..(count - 1), fn i ->
        id = "fn_#{id_start + i}"

        # Randomly select dependencies
        deps =
          if length(dep_candidates) > 0 do
            num_deps = :rand.uniform(min(max_deps, length(dep_candidates)))

            Enum.take_random(dep_candidates, num_deps)
            |> Enum.map(fn dep_id -> {:ref, dep_id} end)
          else
            []
          end

        # Generate a random cost
        cost = min_cost + :rand.uniform(max_cost - min_cost)

        # Generate random resource requirements
        extra_args = generate_random_resources(resource_types)

        # Create function
        %Function{
          id: id,
          args: deps,
          code: fn _inputs -> {:ok, "result for #{id}"} end,
          cost: cost,
          extra_args: extra_args
        }
      end)

    {functions, id_start + count}
  end

  # Generate random resource requirements
  defp generate_random_resources(resource_types) do
    # Randomly select 1-3 resource types
    count = :rand.uniform(3)
    types = Enum.take_random(resource_types, count)

    # Generate requirements for each selected type
    Enum.map(types, fn type ->
      {:resource, type, :rand.uniform(10)}
    end)
  end

  # Generate test node capabilities
  defp generate_test_capabilities(function_count) do
    # Calculate a reasonable number of nodes based on function count
    node_count = max(3, div(function_count, 10))

    # Generate node capabilities
    Enum.map(1..node_count, fn i ->
      node_name = :"node#{i}@localhost"

      # Each node has different resource amounts
      capabilities = %{
        cpu: 4 + :rand.uniform(12),
        memory: 8 + :rand.uniform(24),
        gpu: if(rem(i, 3) == 0, do: :rand.uniform(4), else: 0),
        costs: %{
          compute: 0.5 + :rand.uniform(10) / 10
        }
      }

      {node_name, capabilities}
    end)
    |> Map.new()
  end

  # Benchmark a specific allocator
  defp benchmark_allocator(allocator, dag, caps) do
    start_time = System.monotonic_time(:millisecond)

    # Run allocation
    allocations = allocator.allocate(dag, caps)

    end_time = System.monotonic_time(:millisecond)
    allocation_time = end_time - start_time

    # Calculate metrics
    %{
      allocation_time: allocation_time,
      node_distribution: calculate_node_distribution(allocations, caps),
      load_balance: calculate_load_balance(allocations, dag, caps)
    }
  end

  # Aggregate results from multiple iterations
  defp aggregate_results(iteration_results) do
    # Get keys from first result
    keys = Map.keys(hd(iteration_results))

    # For each key, aggregate the values
    Enum.map(keys, fn key ->
      values = Enum.map(iteration_results, &Map.get(&1, key))

      {key,
       case key do
         :allocation_time ->
           %{
             min: Enum.min(values),
             max: Enum.max(values),
             avg: Enum.sum(values) / length(values)
           }

         :node_distribution ->
           %{
             min_functions: Enum.min_by(values, & &1.min_functions).min_functions,
             max_functions: Enum.max_by(values, & &1.max_functions).max_functions,
             std_dev: Enum.sum(Enum.map(values, & &1.std_dev)) / length(values)
           }

         :load_balance ->
           %{
             min_load: Enum.min_by(values, & &1.min_load).min_load,
             max_load: Enum.max_by(values, & &1.max_load).max_load,
             std_dev: Enum.sum(Enum.map(values, & &1.std_dev)) / length(values)
           }

         _ ->
           values
       end}
    end)
    |> Map.new()
  end

  # Calculate node distribution metrics
  defp calculate_node_distribution(allocations, caps) do
    # Count functions per node
    counts =
      Enum.reduce(allocations, %{}, fn {_, node}, acc ->
        Map.update(acc, node, 1, &(&1 + 1))
      end)

    # Ensure all nodes are represented
    counts =
      Enum.reduce(Map.keys(caps), counts, fn node, acc ->
        Map.put_new(acc, node, 0)
      end)

    # Calculate statistics
    values = Map.values(counts)

    %{
      min_functions: Enum.min(values),
      max_functions: Enum.max(values),
      std_dev: calculate_standard_deviation(values)
    }
  end

  # Calculate load balance metrics
  defp calculate_load_balance(allocations, dag, caps) do
    # Build function map
    function_map =
      Enum.map(dag, fn function -> {function.id, function} end)
      |> Map.new()

    # Calculate load per node
    load =
      Enum.reduce(allocations, %{}, fn {function_id, node}, acc ->
        function = Map.get(function_map, function_id)
        cost = function.cost || 1.0

        Map.update(acc, node, cost, &(&1 + cost))
      end)

    # Ensure all nodes are represented
    load =
      Enum.reduce(Map.keys(caps), load, fn node, acc ->
        Map.put_new(acc, node, 0.0)
      end)

    # Calculate statistics
    values = Map.values(load)

    %{
      min_load: Enum.min(values),
      max_load: Enum.max(values),
      std_dev: calculate_standard_deviation(values)
    }
  end

  # Calculate the success rate of DAG execution
  defp calculate_success_rate(dag_result) do
    functions = dag_result

    succeeded =
      Enum.count(functions, fn function ->
        match?(%{results: {:ok, _}}, function)
      end)

    succeeded / length(functions)
  end

  # Format the summary table for the report
  defp format_summary_table(results) do
    Enum.map(results, fn {config, metrics} ->
      # Handle different module names
      config_name =
        case config do
          mod when is_atom(mod) ->
            mod
            |> to_string()
            |> String.replace("Elixir.Handout.", "")

          other ->
            inspect(other)
        end

      # Get core metrics
      time_metrics = get_in(metrics, [:allocation_time])

      """
      | #{config_name} | #{Float.round(time_metrics.avg, 2)} | n/a | n/a |
      """
    end)
    |> Enum.join("\n")
  end

  # Format detailed results for the report
  defp format_detailed_results(results) do
    Enum.map(results, fn {config, metrics} ->
      # Handle different module names
      config_name =
        case config do
          mod when is_atom(mod) ->
            mod
            |> to_string()
            |> String.replace("Elixir.Handout.", "")

          other ->
            inspect(other)
        end

      """
      ### #{config_name}

      #### Allocation Time (ms)
      - Min: #{metrics.allocation_time.min}
      - Max: #{metrics.allocation_time.max}
      - Avg: #{Float.round(metrics.allocation_time.avg, 2)}

      #### Node Distribution
      - Min Functions per Node: #{metrics.node_distribution.min_functions}
      - Max Functions per Node: #{metrics.node_distribution.max_functions}
      - Standard Deviation: #{Float.round(metrics.node_distribution.std_dev, 2)}

      #### Load Balance
      - Min Load: #{Float.round(metrics.load_balance.min_load, 2)}
      - Max Load: #{Float.round(metrics.load_balance.max_load, 2)}
      - Standard Deviation: #{Float.round(metrics.load_balance.std_dev, 2)}

      """
    end)
    |> Enum.join("\n")
  end

  # Calculate standard deviation of a list of numbers
  defp calculate_standard_deviation(values) do
    mean = Enum.sum(values) / length(values)

    variance =
      Enum.map(values, fn x -> :math.pow(x - mean, 2) end)
      |> Enum.sum()
      |> Kernel./(length(values))

    :math.sqrt(variance)
  end

  # Helper for integer division with remainder
  defp div_rem(a, b) do
    {div(a, b), rem(a, b)}
  end
end
