defmodule Handout.Visualization do
  @moduledoc """
  Provides visualization tools for Handout execution graphs and resource usage.

  This module generates DOT graph format for Graphviz rendering and HTML/JSON
  for web-based visualizations of:

  - Function DAGs with execution status
  - Node resource usage over time
  - Allocation decisions
  - Performance statistics

  ## Usage

  ```elixir
  # Generate a dot graph for a DAG
  dot_graph = Handout.Visualization.dag_to_dot(dag)
  File.write!("graph.dot", dot_graph)

  # Generate HTML report
  html = Handout.Visualization.generate_html_report(dag)
  File.write!("report.html", html)
  ```

  ## Rendering

  The DOT format files can be rendered using Graphviz:
  ```
  dot -Tsvg graph.dot > graph.svg
  ```
  """

  alias Handout.Function
  alias Handout.Telemetry
  alias Handout.Logger, as: HandoutLogger

  @doc """
  Converts a DAG to the DOT graph format for Graphviz.

  ## Parameters
  - `dag`: The DAG to convert, a list of Function structs
  - `options`: Options for controlling graph appearance
    - `:show_status` - Include execution status (default: true)
    - `:show_costs` - Include execution costs (default: true)
    - `:layout` - Graph layout algorithm (default: "dot")
    - `:theme` - Color theme (default: "default")

  ## Returns
  - String in DOT format
  """
  def dag_to_dot(dag, options \\ []) do
    start_time = System.system_time()

    HandoutLogger.info("Generating DOT visualization", function_count: length(dag))

    # Set default options
    options =
      Keyword.merge(
        [
          show_status: true,
          show_costs: true,
          layout: "dot",
          theme: "default"
        ],
        options
      )

    # Get the theme colors
    colors = get_theme_colors(options[:theme])

    # Build the DOT header
    header = """
    digraph G {
      layout=#{options[:layout]};
      rankdir=LR;
      node [style=filled];
      bgcolor="#{colors.background}";

    """

    # Build node definitions
    nodes =
      Enum.map(dag, fn %Function{} = function ->
        node_attrs = build_node_attributes(function, options, colors)
        "  \"#{function.id}\" [#{node_attrs}];"
      end)
      |> Enum.join("\n")

    # Build edge definitions
    edges =
      Enum.flat_map(dag, fn %Function{id: id, args: args} ->
        # Extract function dependencies from args
        deps =
          Enum.filter(args, fn
            {:ref, _} -> true
            _ -> false
          end)
          |> Enum.map(fn {:ref, dep_id} -> dep_id end)

        # Create edges
        Enum.map(deps, fn dep_id ->
          "  \"#{dep_id}\" -> \"#{id}\" [color=\"#{colors.edge}\"];"
        end)
      end)
      |> Enum.join("\n")

    # Combine all parts
    dot_content = header <> nodes <> "\n\n" <> edges <> "\n}\n"

    HandoutLogger.info("DOT visualization generated", size: byte_size(dot_content))

    dot_content
  end

  @doc """
  Generates an HTML report for a DAG execution.

  ## Parameters
  - `dag`: The DAG, a list of Function structs
  - `allocations`: Map of function_id to node
  - `execution_times`: Map of function_id to execution time in ms
  - `options`: Additional options for the report

  ## Returns
  - HTML string containing the complete report
  """
  def generate_html_report(dag, allocations \\ %{}, execution_times \\ %{}, options \\ []) do
    start_time = System.system_time()

    HandoutLogger.info("Generating HTML report", function_count: length(dag))

    # Generate the SVG using dot
    dot_content = dag_to_dot(dag, options)
    svg = dot_to_svg(dot_content)

    # Generate execution statistics
    stats = generate_execution_stats(dag, execution_times)

    # Generate resource allocation visualization
    allocation_chart = generate_allocation_chart(dag, allocations)

    # Create the HTML
    html = """
    <!DOCTYPE html>
    <html>
    <head>
      <title>Handout Execution Report</title>
      <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 5px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
        h1, h2 { color: #333; }
        .graph { overflow: auto; margin: 20px 0; border: 1px solid #ddd; padding: 10px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 15px; margin: 20px 0; }
        .stat-card { background: #f9f9f9; padding: 15px; border-radius: 5px; box-shadow: 0 0 5px rgba(0,0,0,0.05); }
        .chart { margin: 20px 0; height: 400px; border: 1px solid #ddd; padding: 10px; }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th, td { text-align: left; padding: 8px; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
      </style>
    </head>
    <body>
      <div class="container">
        <h1>Handout Execution Report</h1>

        <h2>Execution Graph</h2>
        <div class="graph">
          #{svg}
        </div>

        <h2>Execution Statistics</h2>
        <div class="stats">
          #{stats}
        </div>

        <h2>Resource Allocation</h2>
        <div class="chart">
          #{allocation_chart}
        </div>

        <h2>Function Details</h2>
        <table>
          <tr>
            <th>ID</th>
            <th>Node</th>
            <th>Status</th>
            <th>Time (ms)</th>
            <th>Cost</th>
          </tr>
          #{generate_function_table_rows(dag, allocations, execution_times)}
        </table>
      </div>
    </body>
    </html>
    """

    HandoutLogger.info("HTML report generated", size: byte_size(html))

    html
  end

  @doc """
  Generates a JSON representation of the DAG for web visualizations.

  ## Parameters
  - `dag`: The DAG, a list of Function structs
  - `allocations`: Map of function_id to node
  - `execution_times`: Map of function_id to execution time in ms

  ## Returns
  - JSON string representing the DAG and execution data
  """
  def to_json(dag, allocations \\ %{}, execution_times \\ %{}) do
    # Convert the DAG to a JSON-friendly structure
    nodes =
      Enum.map(dag, fn %Function{} = function ->
        %{
          id: function.id,
          node: Map.get(allocations, function.id, nil),
          status: get_execution_status(function),
          execution_time: Map.get(execution_times, function.id, nil),
          cost: function.cost
        }
      end)

    # Build edges from dependencies
    edges =
      Enum.flat_map(dag, fn %Function{id: id, args: args} ->
        # Extract function dependencies from args
        deps =
          Enum.filter(args, fn
            {:ref, _} -> true
            _ -> false
          end)
          |> Enum.map(fn {:ref, dep_id} -> dep_id end)

        # Create edges
        Enum.map(deps, fn dep_id ->
          %{source: dep_id, target: id}
        end)
      end)

    # Build the full structure
    data = %{
      nodes: nodes,
      edges: edges
    }

    # Convert to JSON
    Jason.encode!(data)
  end

  # Helper functions

  defp get_theme_colors("dark") do
    %{
      background: "#2d2d2d",
      node: "#3c78d8",
      completed: "#6aa84f",
      pending: "#f1c232",
      failed: "#cc0000",
      edge: "#999999",
      text: "#ffffff"
    }
  end

  defp get_theme_colors(_) do
    # Default light theme
    %{
      background: "#ffffff",
      node: "#4285f4",
      completed: "#34a853",
      pending: "#fbbc05",
      failed: "#ea4335",
      edge: "#666666",
      text: "#000000"
    }
  end

  defp build_node_attributes(function, options, colors) do
    # Determine node color based on status
    {color, fontcolor} =
      case get_execution_status(function) do
        :completed -> {colors.completed, "#ffffff"}
        :running -> {colors.pending, "#000000"}
        :failed -> {colors.failed, "#ffffff"}
        _ -> {colors.node, "#ffffff"}
      end

    # Build the label
    label_parts = [function.id]

    # Add cost if enabled
    if options[:show_costs] and function.cost do
      label_parts = ["Cost: #{function.cost}" | label_parts]
    end

    # Add status if enabled
    if options[:show_status] do
      status = get_execution_status(function)
      label_parts = ["Status: #{status}" | label_parts]
    end

    # Combine attributes
    [
      "label=\"#{Enum.join(label_parts, "\\n")}\"",
      "fillcolor=\"#{color}\"",
      "fontcolor=\"#{fontcolor}\""
    ]
    |> Enum.join(", ")
  end

  defp get_execution_status(%Function{} = function) do
    cond do
      function.results == nil -> :pending
      match?({:error, _}, function.results) -> :failed
      match?({:ok, _}, function.results) -> :completed
      true -> :unknown
    end
  end

  defp dot_to_svg(dot_content) do
    # This would typically use System.cmd to call Graphviz
    # For simplicity, we'll use a placeholder here

    # In a real implementation:
    # {svg, 0} = System.cmd("dot", ["-Tsvg"], stdin_data: dot_content)
    # svg

    "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"800\" height=\"600\"><text x=\"10\" y=\"20\">SVG graph would be rendered here with Graphviz.</text></svg>"
  end

  defp generate_execution_stats(dag, execution_times) do
    # Calculate various statistics
    completed_count = Enum.count(dag, fn func -> get_execution_status(func) == :completed end)
    failed_count = Enum.count(dag, fn func -> get_execution_status(func) == :failed end)
    pending_count = Enum.count(dag, fn func -> get_execution_status(func) == :pending end)

    total_time = Enum.sum(Map.values(execution_times))

    avg_time =
      if map_size(execution_times) > 0, do: total_time / map_size(execution_times), else: 0

    # Create HTML for stats cards
    """
    <div class="stat-card">
      <h3>Function Count</h3>
      <p>#{length(dag)}</p>
    </div>
    <div class="stat-card">
      <h3>Completed</h3>
      <p>#{completed_count} (#{percentage(completed_count, length(dag))}%)</p>
    </div>
    <div class="stat-card">
      <h3>Failed</h3>
      <p>#{failed_count} (#{percentage(failed_count, length(dag))}%)</p>
    </div>
    <div class="stat-card">
      <h3>Pending</h3>
      <p>#{pending_count} (#{percentage(pending_count, length(dag))}%)</p>
    </div>
    <div class="stat-card">
      <h3>Total Time</h3>
      <p>#{total_time} ms</p>
    </div>
    <div class="stat-card">
      <h3>Average Time</h3>
      <p>#{Float.round(avg_time, 2)} ms</p>
    </div>
    """
  end

  defp percentage(part, total) when total > 0 do
    Float.round(part / total * 100, 1)
  end

  defp percentage(_, _), do: 0.0

  defp generate_allocation_chart(dag, allocations) do
    # Group functions by node
    functions_by_node =
      Enum.group_by(dag, fn function ->
        Map.get(allocations, function.id, "unallocated")
      end)

    # Count functions per node
    counts_by_node =
      Enum.map(functions_by_node, fn {node, functions} ->
        {node, length(functions)}
      end)
      |> Enum.sort_by(fn {_, count} -> -count end)

    # Create a placeholder for a chart
    # In a real implementation, this would use a charting library
    """
    <svg xmlns="http://www.w3.org/2000/svg" width="800" height="300">
      <text x="10" y="20">Resource allocation chart would be rendered here.</text>
      <text x="10" y="40">#{inspect(counts_by_node)}</text>
    </svg>
    """
  end

  defp generate_function_table_rows(dag, allocations, execution_times) do
    Enum.map(dag, fn function ->
      status = get_execution_status(function)
      node = Map.get(allocations, function.id, "-")
      execution_time = Map.get(execution_times, function.id, "-")
      cost = function.cost || "-"

      """
      <tr>
        <td>#{function.id}</td>
        <td>#{node}</td>
        <td>#{status}</td>
        <td>#{execution_time}</td>
        <td>#{cost}</td>
      </tr>
      """
    end)
    |> Enum.join("\n")
  end
end
