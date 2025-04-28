#!/usr/bin/env elixir
# Simple data processing pipeline example using Handout

# Ensure we can access the Handout library
Code.require_file("../../lib/handout.ex")

defmodule Stats do
  @moduledoc """
  Simple statistics helpers for the pipeline example
  """

  def mean(values) when is_list(values) and length(values) > 0 do
    Enum.sum(values) / length(values)
  end

  def median(values) when is_list(values) and length(values) > 0 do
    sorted = Enum.sort(values)
    mid = div(length(sorted), 2)

    if rem(length(sorted), 2) == 0 do
      (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
    else
      Enum.at(sorted, mid)
    end
  end

  def standard_deviation(values) when is_list(values) and length(values) > 1 do
    avg = mean(values)
    variance = Enum.map(values, fn x -> :math.pow(x - avg, 2) end) |> mean()
    :math.sqrt(variance)
  end
end

# Start Handout
{:ok, _pid} = Handout.start()

# Create a new DAG
dag = Handout.new()

alias Handout.Function

# Step 1: Generate random data
generate_fn = %Function{
  id: :generate_data,
  args: [], # No dependencies
  code: fn ->
    IO.puts("Generating 100 random values...")
    for _ <- 1..100, do: :rand.uniform() * 100
  end
}

# Step 2: Filter data
filter_fn = %Function{
  id: :filter_data,
  args: [:generate_data], # Depends on generate_data
  code: fn %{generate_data: data}, threshold ->
    IO.puts("Filtering values > #{threshold}...")
    Enum.filter(data, fn x -> x > threshold end)
  end,
  extra_args: [30] # Filter threshold
}

# Step 3: Transform data
transform_fn = %Function{
  id: :transform_data,
  args: [:filter_data], # Depends on filter_data
  code: fn %{filter_data: data}, operation ->
    IO.puts("Applying #{operation} transformation...")
    case operation do
      :square -> Enum.map(data, fn x -> x * x end)
      :sqrt -> Enum.map(data, fn x -> :math.sqrt(x) end)
      :log -> Enum.map(data, fn x -> :math.log(x) end)
      _ -> data # Default: no transformation
    end
  end,
  extra_args: [:square] # Operation to apply
}

# Step 4: Aggregate results
aggregate_fn = %Function{
  id: :aggregate_data,
  args: [:transform_data], # Depends on transform_data
  code: fn %{transform_data: data} ->
    IO.puts("Calculating statistics...")
    %{
      count: length(data),
      min: Enum.min(data),
      max: Enum.max(data),
      mean: Stats.mean(data),
      median: Stats.median(data),
      stddev: Stats.standard_deviation(data)
    }
  end
}

# Step 5: Format output
format_fn = %Function{
  id: :format_output,
  args: [:aggregate_data, :filter_data], # Multiple dependencies!
  code: fn %{aggregate_data: stats, filter_data: original_data} ->
    IO.puts("Formatting results...")
    """

    DATA PIPELINE RESULTS
    ---------------------
    Original filtered data count: #{length(original_data)}

    STATISTICS:
      Count:  #{stats.count}
      Min:    #{Float.round(stats.min, 2)}
      Max:    #{Float.round(stats.max, 2)}
      Mean:   #{Float.round(stats.mean, 2)}
      Median: #{Float.round(stats.median, 2)}
      StdDev: #{Float.round(stats.stddev, 2)}
    """
  end
}

# Build the DAG
dag =
  dag
  |> Handout.DAG.add_function(generate_fn)
  |> Handout.DAG.add_function(filter_fn)
  |> Handout.DAG.add_function(transform_fn)
  |> Handout.DAG.add_function(aggregate_fn)
  |> Handout.DAG.add_function(format_fn)

# Validate the DAG
case Handout.DAG.validate(dag) do
  {:ok, valid_dag} ->
    IO.puts("\nExecuting data pipeline...\n")

    # Execute the DAG
    case Handout.execute(valid_dag) do
      {:ok, results} ->
        # Print the final formatted output
        IO.puts(results[:format_output])

      {:error, reason} ->
        IO.puts("Error executing pipeline: #{inspect(reason)}")
    end

  {:error, reason} ->
    IO.puts("Invalid DAG: #{inspect(reason)}")
end
