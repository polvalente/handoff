defmodule Handout.SimpleAllocator do
  @moduledoc """
  A simple implementation of the Allocator behavior that provides
  basic allocation strategies for distributing functions across nodes.

  Supports allocation strategies:
  - first_available: assigns to the first node with available resources
  - load_balanced: distributes functions across nodes to balance load
  """

  @behaviour Handout.Allocator

  alias Handout.Function
  alias Handout.SimpleResourceTracker

  @doc """
  Allocate functions to nodes based on resource requirements and node capabilities.
  Uses the first_available strategy by default.

  ## Parameters
  - `functions`: List of functions to allocate
  - `caps`: Map of node capabilities in the format %{node() => capabilities_map}

  ## Returns
  A map with function IDs as keys and node assignments as values.
  """
  @impl Handout.Allocator
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
    # Sort nodes for consistent allocation order
    nodes = caps |> Map.keys() |> Enum.sort()

    # Process each function and assign to the first available node
    Enum.reduce(functions, %{}, fn %Function{id: id, cost: cost}, assignments ->
      # Skip if function has no resource requirements
      if is_nil(cost) || cost == %{} do
        Map.put(assignments, id, List.first(nodes))
      else
        # Find first node with available resources
        assigned_node =
          Enum.find(nodes, fn node ->
            SimpleResourceTracker.available?(node, cost)
          end)

        if assigned_node do
          Map.put(assignments, id, assigned_node)
        else
          # If no node has resources, assign to first node anyway
          # (will fail at execution time)
          Map.put(assignments, id, List.first(nodes))
        end
      end
    end)
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
      Enum.reduce(sorted_functions, {%{}, initial_loads}, fn %Function{id: id, cost: cost},
                                                             {assignments, loads} ->
        # Find node with least load that can handle this function
        candidates =
          if is_nil(cost) || cost == %{} do
            # If no resource requirements, consider all nodes
            Enum.map(nodes, fn node -> {node, Map.get(loads, node, 0)} end)
          else
            # Filter nodes that have available resources
            Enum.filter(nodes, fn node ->
              SimpleResourceTracker.available?(node, cost)
            end)
            |> Enum.map(fn node -> {node, Map.get(loads, node, 0)} end)
          end

        # Sort by current load
        sorted_candidates = Enum.sort_by(candidates, fn {_node, load} -> load end)

        case sorted_candidates do
          [] ->
            # If no suitable node, assign to node with least load
            {node, _} =
              Enum.min_by(
                Enum.map(nodes, fn n -> {n, Map.get(loads, n, 0)} end),
                fn {_, load} -> load end
              )

            new_load =
              Map.update(loads, node, resource_weight(cost), &(&1 + resource_weight(cost)))

            {Map.put(assignments, id, node), new_load}

          [{node, _} | _] ->
            # Assign to node with least load
            new_load =
              Map.update(loads, node, resource_weight(cost), &(&1 + resource_weight(cost)))

            {Map.put(assignments, id, node), new_load}
        end
      end)

    assignments
  end

  # Helper to calculate a weighted resource value
  defp resource_weight(nil), do: 0
  defp resource_weight(cost) when map_size(cost) == 0, do: 0

  defp resource_weight(cost) do
    Enum.reduce(cost, 0, fn {_, amount}, total -> total + amount end)
  end
end
