defmodule Handout.CostOptimizedAllocator do
  @moduledoc """
  An allocator that optimizes for overall cost when distributing functions to nodes.

  This allocator takes into account:
  - Function execution costs
  - Node resource costs/pricing
  - Network transfer costs between nodes
  - Current node load

  It attempts to find an allocation that minimizes total execution cost.
  """

  @behaviour Handout.Allocator

  alias Handout.Function
  alias Handout.Logger, as: HandoutLogger
  alias Handout.Telemetry

  @doc """
  Allocate functions to nodes, optimizing for overall cost.

  ## Parameters
  - `functions`: List of functions to allocate
  - `caps`: Map of node capabilities (node => capabilities)

  ## Returns
  - Map of allocations (function_id => node)
  """
  @impl Handout.Allocator
  def allocate(functions, caps) do
    start_time = System.system_time()

    HandoutLogger.info("Starting cost-optimized allocation",
      function_count: length(functions),
      node_count: map_size(caps)
    )

    # Emit telemetry start event
    Telemetry.emit_start_event(
      Telemetry.allocator_allocation_start(),
      %{
        function_count: length(functions),
        node_count: map_size(caps),
        allocator: __MODULE__
      }
    )

    # Extract node costs
    node_costs = extract_node_costs(caps)

    # Get dependency graph
    dependency_graph = build_dependency_graph(functions)

    # Calculate the cost of running each function on each node
    function_costs = calculate_function_costs(functions, caps, node_costs)

    # Prioritize functions by topology and cost
    sorted_functions = topological_sort(functions, dependency_graph)

    # Assign functions to nodes
    allocations = assign_functions(sorted_functions, function_costs, caps)

    # Emit telemetry stop event
    Telemetry.emit_stop_event(
      Telemetry.allocator_allocation_stop(),
      start_time,
      %{
        function_count: length(functions),
        node_count: map_size(caps),
        allocator: __MODULE__,
        allocations: allocations
      }
    )

    HandoutLogger.info("Completed cost-optimized allocation",
      function_count: length(functions),
      allocation_count: map_size(allocations)
    )

    allocations
  end

  # Helper functions

  # Extract cost information from node capabilities
  defp extract_node_costs(caps) do
    Enum.map(caps, fn {node, node_caps} ->
      cost_map = Map.get(node_caps, :costs, %{})
      {node, cost_map}
    end)
    |> Map.new()
  end

  # Build a dependency graph from functions
  defp build_dependency_graph(functions) do
    # Initialize the graph with empty dependency sets
    initial_graph =
      Enum.map(functions, fn %Function{id: id} -> {id, MapSet.new()} end)
      |> Map.new()

    # Add dependencies
    Enum.reduce(functions, initial_graph, fn %Function{id: id, args: args}, graph ->
      # Find function IDs in args
      deps =
        Enum.filter(args, fn
          {:ref, _dep_id} -> true
          _ -> false
        end)
        |> Enum.map(fn {:ref, dep_id} -> dep_id end)
        |> MapSet.new()

      Map.put(graph, id, deps)
    end)
  end

  # Calculate the cost of running each function on each node
  defp calculate_function_costs(functions, caps, node_costs) do
    Enum.map(functions, fn function ->
      # Get function cost (default to 1.0 if not specified)
      function_cost = Map.get(function, :cost, 1.0)

      # Calculate cost for each node
      node_function_costs =
        Enum.map(caps, fn {node, _node_caps} ->
          # Get node's cost multiplier for this resource type (default to 1.0)
          cost_multiplier = Map.get(node_costs[node], :compute, 1.0)

          # Combine function and node costs
          total_cost = function_cost * cost_multiplier

          {node, total_cost}
        end)
        |> Map.new()

      {function.id, node_function_costs}
    end)
    |> Map.new()
  end

  # Sort functions topologically to respect dependencies
  defp topological_sort(functions, dependency_graph) do
    # Build a reverse lookup of function ID to function
    function_map =
      Enum.map(functions, fn function -> {function.id, function} end)
      |> Map.new()

    # Initialize visited and result
    visited = MapSet.new()
    result = []

    # Process each function
    {_, sorted} =
      Enum.reduce(functions, {visited, result}, fn function, {visited, result} ->
        visit_node(function.id, dependency_graph, function_map, visited, result)
      end)

    # Return functions in topological order
    sorted
  end

  # DFS traversal for topological sort
  defp visit_node(node_id, graph, function_map, visited, result) do
    if MapSet.member?(visited, node_id) do
      {visited, result}
    else
      visited = MapSet.put(visited, node_id)

      # Visit all dependencies first
      deps = Map.get(graph, node_id, MapSet.new())

      {visited, result} =
        Enum.reduce(deps, {visited, result}, fn dep_id, {visited, result} ->
          visit_node(dep_id, graph, function_map, visited, result)
        end)

      # Add current node to result
      function = Map.get(function_map, node_id)
      {visited, [function | result]}
    end
  end

  # Assign functions to nodes based on cost and dependencies
  defp assign_functions(sorted_functions, function_costs, caps) do
    # Track allocations and current load
    initial_state = %{
      allocations: %{},
      node_load: initialize_node_load(caps)
    }

    # For each function in order
    Enum.reduce(sorted_functions, initial_state, fn function, state ->
      # Find the best node for this function
      best_node = find_best_node(function, function_costs, state, caps)

      # Update allocations
      allocations = Map.put(state.allocations, function.id, best_node)

      # Update node load
      node_load =
        Map.update!(state.node_load, best_node, fn load ->
          load + (function.cost || 1.0)
        end)

      %{allocations: allocations, node_load: node_load}
    end)
    |> Map.get(:allocations)
  end

  # Initialize node load to zero for all nodes
  defp initialize_node_load(caps) do
    Enum.map(caps, fn {node, _} -> {node, 0.0} end)
    |> Map.new()
  end

  # Find the best node for a function, considering cost and load
  defp find_best_node(function, function_costs, state, caps) do
    function_id = function.id

    # Get cost map for this function
    node_costs = Map.get(function_costs, function_id, %{})

    # Find node with lowest adjusted cost (considering current load)
    Enum.map(node_costs, fn {node, cost} ->
      # Skip nodes that don't have enough resources
      capabilities = Map.get(caps, node, %{})
      requirements = extract_requirements(function)

      if has_resources?(capabilities, requirements) do
        # Adjust cost by node load
        current_load = Map.get(state.node_load, node, 0.0)
        # Avoid overloading nodes
        load_factor = 1.0 + current_load / 10.0
        adjusted_cost = cost * load_factor

        {node, adjusted_cost}
      else
        {node, :infinity}
      end
    end)
    |> Enum.min_by(fn
      {_, :infinity} -> :infinity
      {_, cost} -> cost
    end)
    |> elem(0)
  end

  # Extract resource requirements from a function
  defp extract_requirements(function) do
    # Resource requirements are specified in extra_args as {:resource, resource_type, amount}
    Enum.filter(function.extra_args || [], fn
      {:resource, _, _} -> true
      _ -> false
    end)
    |> Enum.map(fn {:resource, type, amount} -> {type, amount} end)
    |> Map.new()
  end

  # Check if a node has enough resources for a function
  defp has_resources?(capabilities, requirements) do
    Enum.all?(requirements, fn {resource, amount} ->
      available = Map.get(capabilities, resource, 0)
      available >= amount
    end)
  end
end
