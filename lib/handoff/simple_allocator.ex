defmodule Handoff.SimpleAllocator do
  @moduledoc """
  A simple implementation of the Allocator behavior that provides
  basic allocation strategies for distributing functions across nodes.
  """

  @behaviour Handoff.Allocator

  alias Handoff.Allocator.AllocationError
  alias Handoff.Function

  @doc """
  Allocate functions to nodes based on resource requirements and node capabilities.

  ## Parameters
  - `functions`: List of functions to allocate
  - `caps`: Map of node capabilities in the format %{node() => capabilities_map}

  ## Returns
  A map with function IDs as keys and node assignments as values.
  """
  @impl Handoff.Allocator
  def allocate(functions, caps) do
    # Sort nodes for consistent allocation order, prioritizing Node.self()
    all_nodes = caps |> Map.keys() |> Enum.sort()
    self_node = Node.self()

    nodes =
      if self_node in all_nodes do
        [self_node | List.delete(all_nodes, self_node)]
      else
        # If Node.self() is not in caps, log a warning or handle as per desired behavior.
        # For now, just use the sorted list of all_nodes.
        # IO.inspect("Warning: Node.self() (#{inspect(self_node)}) not found in capabilities map. Defaulting to standard sort.", label: "SimpleAllocator")
        all_nodes
      end

    # Initialize available resources for each node based on initial capabilities
    # This map will be updated as functions are assigned.
    initial_available_resources =
      Enum.reduce(nodes, %{}, fn node, acc ->
        # Assuming caps is %{node_pid => %{cpu: x, memory: y}}
        # If a node isn't in caps or has no resources defined, default to 0 for safety.
        # However, SimpleResourceTracker.register ensures nodes have some capabilities.
        # For this logic, we'll directly use what's in caps.
        # If caps could be sparse or incomplete, more robust fetching would be needed here.
        Map.put(acc, node, Map.get(caps, node, %{cpu: 0, memory: 0}))
      end)

    # Partition functions: those with a pre-defined node and those for dynamic allocation
    {pinned_functions, dynamic_functions} =
      Enum.split_with(functions, fn %Function{node: node} -> not is_nil(node) end)

    {collocated_functions, pinned_functions} =
      Enum.split_with(pinned_functions, fn
        %Function{node: {:collocated, _}} -> true
        _ -> false
      end)

    {pinned_functions, remapped_collocations_pinned, to_collocate_functions} =
      merge_collocated_costs(pinned_functions, collocated_functions)

    {dynamic_functions, remapped_collocations_dynamic, []} =
      merge_collocated_costs(dynamic_functions, to_collocate_functions)

    collocated_functions = remapped_collocations_pinned ++ remapped_collocations_dynamic

    # Process pinned functions first
    {pinned_assignments, available_resources_after_pinning} =
      perform_pinned_allocation(pinned_functions, %{}, initial_available_resources)

    # Process remaining (dynamic) functions
    {dynamic_assignments_map, available_resources_after_dynamic_allocation, _dynamic_nodes} =
      Enum.reduce(
        # Use functions not already pinned
        dynamic_functions,
        # Start with results from pinned
        {pinned_assignments, available_resources_after_pinning, nodes},
        fn
          %Function{id: id, cost: cost},
          {current_assignments, available_resources, current_nodes_list} ->
            if is_nil(cost) || cost == %{} do
              assigned_node = List.first(current_nodes_list)

              {Map.put(current_assignments, id, assigned_node), available_resources,
               current_nodes_list}
            else
              find_node_assignment(
                id,
                current_assignments,
                available_resources,
                cost,
                current_nodes_list
              )
            end
        end
      )

    {final_assignments_map, _} =
      collocated_functions
      |> Enum.map(fn %Function{node: {:collocated, target_id}} = function ->
        target_node = Map.get(dynamic_assignments_map, target_id)
        %{function | node: target_node}
      end)
      |> perform_pinned_allocation(
        dynamic_assignments_map,
        available_resources_after_dynamic_allocation
      )

    final_assignments_map
  end

  defp perform_pinned_allocation(functions, assignments, available_resources) do
    Enum.reduce(
      functions,
      {assignments, available_resources},
      fn %Function{id: id, cost: cost, node: assigned_node}, {acc_assignments, acc_resources} ->
        # Assume assigned_node is valid and its resources are tracked.
        # If the node is not in caps (and thus not in acc_resources initially),
        # this might indicate an issue or a node without declared capacity.
        # For now, we proceed assuming it's a known node.
        # If cost is nil, treat as no resource requirement for subtraction.
        cost = cost || %{}
        node_current_resources = Map.get(acc_resources, assigned_node, %{cpu: 0, memory: 0})

        if not can_allocate?(node_current_resources, cost) do
          raise AllocationError,
                "Insufficient resources on node #{inspect(assigned_node)} for function #{inspect(id)}"
        end

        # For pinned functions, we assign them regardless of can_allocate? outcome,
        # as pinning implies a directive. Resources are subtracted.
        updated_node_res = subtract_resources(node_current_resources, cost)
        new_resources = Map.put(acc_resources, assigned_node, updated_node_res)
        new_assignments = Map.put(acc_assignments, id, assigned_node)
        {new_assignments, new_resources}
      end
    )
  end

  defp find_node_assignment(id, current_assignments, available_resources, cost, nodes) do
    found_node_assignment_tuple =
      Enum.find_value(nodes, fn node ->
        node_current_resources = Map.get(available_resources, node)

        if can_allocate?(node_current_resources, cost) do
          # Node found, update assignments and subtract resources for this node
          new_assignments = Map.put(current_assignments, id, node)
          updated_node_resources = subtract_resources(node_current_resources, cost)

          new_available_resources =
            Map.put(available_resources, node, updated_node_resources)

          # Move the chosen node to the front of the list
          updated_nodes_list = [node | List.delete(nodes, node)]

          # Return value for Enum.find_value: {assignments, resources, updated_nodes}
          {new_assignments, new_available_resources, updated_nodes_list}
        end
      end)

    if found_node_assignment_tuple do
      # Node was found and resources/nodes list were updated
      found_node_assignment_tuple
    else
      # If no node has resources, assign to first node (original fallback)
      # and don't alter available_resources for this function assignment.
      # Move the fallback node to the front of the list.
      assigned_node = List.first(nodes)

      {Map.put(current_assignments, id, assigned_node), available_resources, nodes}
    end
  end

  # Helper function to check if node_resources can satisfy function_cost
  # Assumes resources are maps like %{cpu: x, memory: y}
  defp can_allocate?(node_resources, function_cost)
       when is_map(node_resources) and is_map(function_cost) do
    keys = Enum.uniq(Map.keys(function_cost) ++ Map.keys(node_resources))

    Enum.all?(keys, fn key ->
      available = node_resources[key] || 0
      required = function_cost[key] || 0

      available >= required
    end)
  end

  # Helper function to subtract function_cost from node_resources
  defp subtract_resources(node_resources, function_cost)
       when is_map(function_cost) and is_map(node_resources) do
    keys = Enum.uniq(Map.keys(function_cost) ++ Map.keys(node_resources))

    Map.new(keys, fn key ->
      node_resource = node_resources[key] || 0
      function_cost = function_cost[key] || 0
      result = max(0, node_resource - function_cost)

      {key, result}
    end)
  end

  defp merge_collocated_costs([], collocated_functions) do
    {[], [], collocated_functions}
  end

  defp merge_collocated_costs(functions, []) do
    {functions, [], []}
  end

  defp merge_collocated_costs(functions, collocated_functions) do
    g = build_collocation_graph(functions, collocated_functions)
    collocated_by_target = extract_collocation_components(g)

    updated_functions = merge_costs_into_functions(functions, collocated_by_target)
    remapped_collocations = build_remapping_table(collocated_by_target)

    {remapped, to_collocate} =
      partition_collocated_functions(collocated_functions, remapped_collocations)

    {updated_functions, remapped, to_collocate}
  end

  defp build_collocation_graph(functions, collocated_functions) do
    g = :digraph.new()

    # Add root vertices for all functions
    for %{id: id} <- functions do
      :digraph.add_vertex(g, id, :root)
    end

    # Add collocated functions and their edges
    for %{id: id, node: {:collocated, target_id}, cost: cost} <- collocated_functions do
      add_vertex_if_missing(g, id, cost)
      add_vertex_if_missing(g, target_id, cost)
      :digraph.add_edge(g, id, target_id)
    end

    g
  end

  defp add_vertex_if_missing(graph, vertex_id, cost) do
    unless :digraph.vertex(graph, vertex_id) do
      :digraph.add_vertex(graph, vertex_id, cost)
    end
  end

  defp extract_collocation_components(graph) do
    for component <- :digraph_utils.components(graph), into: %{} do
      {[{root_id, :root}], collocations} =
        component
        |> Enum.map(&:digraph.vertex(graph, &1))
        |> Enum.split_with(fn {_, label} -> label == :root end)

      {root_id, collocations}
    end
  end

  defp merge_costs_into_functions(functions, collocated_by_target) do
    Enum.map(functions, fn %{id: id, cost: cost} = function ->
      case Map.get(collocated_by_target, id) do
        nil -> function
        collocations -> %{function | cost: calculate_merged_cost(cost, collocations)}
      end
    end)
  end

  defp calculate_merged_cost(base_cost, collocations) do
    Enum.reduce(collocations, base_cost || %{}, fn {_id, cost}, acc ->
      merge_cost_if_present(acc, cost)
    end)
  end

  defp merge_cost_if_present(acc, nil), do: acc

  defp merge_cost_if_present(acc, cost) do
    Map.merge(acc, cost, fn _key, v1, v2 -> v1 + v2 end)
  end

  defp build_remapping_table(collocated_by_target) do
    collocated_by_target
    |> Enum.flat_map(fn {id, collocations} ->
      Enum.map(collocations, fn {collocated_id, _cost} ->
        {collocated_id, id}
      end)
    end)
    |> Map.new()
  end

  defp partition_collocated_functions(collocated_functions, remapped_collocations) do
    Enum.reduce(collocated_functions, {[], []}, fn f, {remapped, to_collocate} ->
      case Map.get(remapped_collocations, f.id) do
        nil -> {remapped, [f | to_collocate]}
        target_id -> {[%{f | node: {:collocated, target_id}, cost: nil} | remapped], to_collocate}
      end
    end)
  end
end
