defmodule Handoff.SimpleAllocator do
  @moduledoc """
  A simple implementation of the Allocator behavior that provides
  basic allocation strategies for distributing functions across nodes.

  Supports allocation strategies:
  - first_available: assigns to the first node with available resources
  - load_balanced: distributes functions across nodes to balance load
  """

  @behaviour Handoff.Allocator

  alias Handoff.Function
  alias Handoff.SimpleResourceTracker

  @doc """
  Allocate functions to nodes based on resource requirements and node capabilities.
  Uses the first_available strategy by default.

  ## Parameters
  - `functions`: List of functions to allocate
  - `caps`: Map of node capabilities in the format %{node() => capabilities_map}

  ## Returns
  A map with function IDs as keys and node assignments as values.
  """
  @impl Handoff.Allocator
  def allocate(functions, caps) do
    allocate(functions, caps, :first_available)
  end

  @doc """
  Allocate functions to nodes with a specific strategy.

  ## Parameters
  - `functions`: List of functions to allocate
  - `caps`: Map of node capabilities in the format %{node() => capabilities_map}
  - `strategy`: The allocation strategy to use (:first_available or :load_balanced)

  ## Returns
  A map with function IDs as keys and node assignments as values.
  """
  def allocate(functions, caps, strategy) when strategy in [:first_available, :load_balanced] do
    case strategy do
      :first_available -> first_available_allocation(functions, caps)
      :load_balanced -> load_balanced_allocation(functions, caps)
    end
  end

  # Private functions for allocation strategies

  defp first_available_allocation(functions, caps) do
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

    # Process each function and assign to the first available node
    {assignments, _final_resources, _final_nodes} =
      Enum.reduce(functions, {%{}, initial_available_resources, nodes}, fn
        %Function{id: id, cost: cost},
        {current_assignments, available_resources, current_nodes_list} ->
          # If function has no resource requirements, assign to the first node by default
          # and don't alter available resources for this function.
          # The node list is updated to move the assigned node to the front.
          if is_nil(cost) || cost == %{} do
            assigned_node = List.first(current_nodes_list)

            {Map.put(current_assignments, id, assigned_node), available_resources,
             current_nodes_list}
          else
            # Find first node that can satisfy the cost based on *current* available_resources
            # and get the updated nodes list
            find_node_assignment(
              id,
              current_assignments,
              available_resources,
              cost,
              # Pass current_nodes_list
              current_nodes_list
            )
          end
      end)

    assignments
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
  defp can_allocate?(node_resources, function_cost) do
    # Ensure both cpu and memory are present or default to 0 if missing in cost
    required_cpu = Map.get(function_cost, :cpu, 0)
    required_memory = Map.get(function_cost, :memory, 0)

    # Ensure node_resources also default if a key is missing (shouldn't happen with proper init)
    available_cpu = Map.get(node_resources, :cpu, 0)
    available_memory = Map.get(node_resources, :memory, 0)

    available_cpu >= required_cpu && available_memory >= required_memory
  end

  # Helper function to subtract function_cost from node_resources
  defp subtract_resources(node_resources, function_cost) do
    # This function assumes can_allocate? was true before calling
    updated_cpu = Map.get(node_resources, :cpu, 0) - Map.get(function_cost, :cpu, 0)
    updated_memory = Map.get(node_resources, :memory, 0) - Map.get(function_cost, :memory, 0)

    # Ensure resources don't go negative, though can_allocate? should prevent this.
    %{cpu: max(0, updated_cpu), memory: max(0, updated_memory)}
  end

  defp load_balanced_allocation(functions, caps) do
    # Sort functions by resource cost (highest first)
    sorted_functions =
      Enum.sort_by(
        functions,
        fn %Function{cost: cost} ->
          if is_nil(cost), do: 0, else: resource_weight(cost)
        end,
        :desc
      )

    # Track node loads for balancing
    nodes = caps |> Map.keys() |> Enum.sort()
    initial_loads = Enum.reduce(nodes, %{}, fn node, acc -> Map.put(acc, node, 0) end)

    # Allocate functions to balance load
    {assignments, _} =
      Enum.reduce(sorted_functions, {%{}, initial_loads}, &perform_allocation(&1, &2, nodes))

    assignments
  end

  defp perform_allocation(%Function{id: id, cost: cost}, {assignments, loads}, nodes) do
    # Find node with least load that can handle this function
    candidates =
      if is_nil(cost) || map_size(cost) == 0 do
        # If no resource requirements, consider all nodes
        Enum.map(nodes, fn node -> {node, Map.get(loads, node, 0)} end)
      else
        # Filter nodes that have available resources
        nodes
        |> Enum.filter(fn node ->
          SimpleResourceTracker.available?(node, cost)
        end)
        |> Enum.map(fn node -> {node, Map.get(loads, node, 0)} end)
      end

    # Sort by current load
    sorted_candidates = Enum.sort_by(candidates, fn {_node, load} -> load end)

    case sorted_candidates do
      [] ->
        # If no suitable node, assign to node with least load
        {node, _} = Enum.min_by(nodes, fn n -> Map.get(loads, n, 0) end)

        new_load = Map.update(loads, node, resource_weight(cost), &(&1 + resource_weight(cost)))

        {Map.put(assignments, id, node), new_load}

      [{node, _} | _] ->
        # Assign to node with least load
        new_load =
          Map.update(loads, node, resource_weight(cost), &(&1 + resource_weight(cost)))

        {Map.put(assignments, id, node), new_load}
    end
  end

  # Helper to calculate a weighted resource value
  defp resource_weight(nil), do: 0
  defp resource_weight(cost) when map_size(cost) == 0, do: 0

  defp resource_weight(cost) do
    Enum.reduce(cost, 0, fn {_, amount}, total -> total + amount end)
  end
end
