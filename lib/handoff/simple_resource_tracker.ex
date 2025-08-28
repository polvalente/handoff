defmodule Handoff.SimpleResourceTracker do
  @moduledoc """
  A simple implementation of the ResourceTracker behavior that tracks
  resources using an ETS table.
  """

  @behaviour Handoff.ResourceTracker

  use GenServer

  require Logger

  @table_name :handoff_resources

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Handoff.ResourceTracker
  def register(node, caps) do
    GenServer.call(__MODULE__, {:register, node, caps})
  end

  @impl Handoff.ResourceTracker
  def available?(node, req) do
    GenServer.call(__MODULE__, {:available?, node, req})
  end

  @impl Handoff.ResourceTracker
  def request(node, req) do
    GenServer.call(__MODULE__, {:request, node, req})
  end

  @impl Handoff.ResourceTracker
  def release(node, req) do
    GenServer.call(__MODULE__, {:release, node, req})
  end

  @doc """
  Returns the capabilities of the local node.

  Used for remote calls from other nodes to discover this node's capabilities.

  ## Returns
  - Map of resource capabilities
  """
  def get_capabilities do
    GenServer.call(__MODULE__, :get_capabilities)
  end

  # Server callbacks

  @impl GenServer
  def init(_opts) do
    table = :ets.new(@table_name, [:set, :protected, :named_table])

    {:ok,
     %{
       table: table,
       nodes: %{},
       # Track which process allocated which resources on which nodes
       # %{pid => %{node => resource_requirements}}
       process_allocations: %{},
       # Track monitors for processes
       # %{monitor_ref => pid}
       monitors: %{}
     }}
  end

  @impl GenServer
  def handle_call({:register, node, caps}, _from, state) do
    # Store the full capacity
    full_caps = caps

    # Initialize used resources as empty
    used = %{}

    # Store node info in state
    nodes = Map.put(state.nodes, node, %{full: full_caps, used: used})

    Logger.info("Registered node #{inspect(node)} with capabilities: #{inspect(caps)}")

    {:reply, :ok, %{state | nodes: nodes}}
  end

  @impl GenServer
  def handle_call({:available?, node, req}, _from, state) do
    case Map.get(state.nodes, node) do
      nil ->
        {:reply, false, state}

      %{full: full_caps, used: used} ->
        # Check if all required resources are available
        available = check_resource_availability(full_caps, used, req)
        {:reply, available, state}
    end
  end

  @impl GenServer
  def handle_call({:request, node, req}, {from_pid, _tag}, state) do
    case Map.get(state.nodes, node) do
      nil ->
        {:reply, {:error, :resources_unavailable}, state}

      %{full: full_caps, used: used} ->
        if check_resource_availability(full_caps, used, req) do
          # Update used resources
          new_used = update_used_resources(used, req, :add)

          nodes =
            Map.update!(state.nodes, node, fn node_info ->
              %{node_info | used: new_used}
            end)

          # Monitor the requesting process and track its allocation
          {new_process_allocations, new_monitors} =
            track_process_allocation(
              from_pid,
              node,
              req,
              state.process_allocations,
              state.monitors
            )

          Logger.debug(
            "Allocated resources on node #{inspect(node)} to process #{inspect(from_pid)}: #{inspect(req)}"
          )

          {:reply, :ok,
           %{
             state
             | nodes: nodes,
               process_allocations: new_process_allocations,
               monitors: new_monitors
           }}
        else
          {:reply, {:error, :resources_unavailable}, state}
        end
    end
  end

  @impl GenServer
  def handle_call({:release, node, req}, {from_pid, _tag}, state) do
    case Map.get(state.nodes, node) do
      nil ->
        {:reply, :ok, state}

      %{used: used} ->
        # Update used resources by reducing
        new_used = update_used_resources(used, req, :remove)

        nodes =
          Map.update!(state.nodes, node, fn node_info ->
            %{node_info | used: new_used}
          end)

        # Clean up process allocation tracking
        {new_process_allocations, new_monitors} =
          untrack_process_allocation(
            from_pid,
            node,
            req,
            state.process_allocations,
            state.monitors
          )

        Logger.debug(
          "Released resources on node #{inspect(node)} from process #{inspect(from_pid)}: #{inspect(req)}"
        )

        {:reply, :ok,
         %{
           state
           | nodes: nodes,
             process_allocations: new_process_allocations,
             monitors: new_monitors
         }}
    end
  end

  @impl GenServer
  def handle_call(:get_capabilities, _from, state) do
    # Get capabilities of local node (Node.self())
    case Map.get(state.nodes, Node.self()) do
      nil ->
        {:reply, %{}, state}

      %{full: full_caps} ->
        {:reply, full_caps, state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, monitor_ref, :process, pid, reason}, state) do
    Logger.info(
      "Process #{inspect(pid)} died (#{inspect(reason)}), cleaning up its resource allocations"
    )

    # Clean up all resources allocated by this process
    {new_nodes, new_process_allocations, new_monitors} =
      cleanup_process_resources(
        pid,
        monitor_ref,
        state.nodes,
        state.process_allocations,
        state.monitors
      )

    {:noreply,
     %{
       state
       | nodes: new_nodes,
         process_allocations: new_process_allocations,
         monitors: new_monitors
     }}
  end

  # Helper functions

  defp check_resource_availability(full_caps, used, req) do
    Enum.all?(req, fn {resource, amount} ->
      total = Map.get(full_caps, resource, 0)
      current_used = Map.get(used, resource, 0)
      total - current_used >= amount
    end)
  end

  defp update_used_resources(used, req, operation) do
    Enum.reduce(req, used, fn {resource, amount}, acc ->
      current = Map.get(acc, resource, 0)

      new_value =
        case operation do
          :add -> current + amount
          :remove -> max(0, current - amount)
        end

      Map.put(acc, resource, new_value)
    end)
  end

  defp track_process_allocation(pid, node, req, process_allocations, monitors) do
    # Check if we're already monitoring this process
    if Map.has_key?(process_allocations, pid) do
      # Already monitoring, just add the allocation
      updated_allocations =
        Map.update!(process_allocations, pid, fn node_allocations ->
          Map.update(node_allocations, node, req, fn existing_req ->
            # Merge the resource requirements
            Map.merge(existing_req, req, fn _key, v1, v2 -> v1 + v2 end)
          end)
        end)

      {updated_allocations, monitors}
    else
      # Start monitoring this process
      monitor_ref = Process.monitor(pid)
      new_process_allocations = Map.put(process_allocations, pid, %{node => req})
      new_monitors = Map.put(monitors, monitor_ref, pid)
      {new_process_allocations, new_monitors}
    end
  end

  defp untrack_process_allocation(pid, node, req, process_allocations, monitors) do
    with {:ok, node_allocations} <- Map.fetch(process_allocations, pid),
         {:ok, existing_req} <- Map.fetch(node_allocations, node) do
      # Subtract the released resources
      new_req = Map.merge(existing_req, req, fn _key, v1, v2 -> max(0, v1 - v2) end)

      # Remove zero allocations
      cleaned_req = Enum.reject(new_req, fn {_resource, amount} -> amount == 0 end) |> Map.new()

      if map_size(cleaned_req) == 0 do
        # No more allocations on this node
        updated_node_allocations = Map.delete(node_allocations, node)

        if map_size(updated_node_allocations) == 0 do
          # No more allocations for this process, stop monitoring
          stop_monitoring_process(pid, process_allocations, monitors)
        else
          # Still has allocations on other nodes
          new_process_allocations =
            Map.put(process_allocations, pid, updated_node_allocations)

          {new_process_allocations, monitors}
        end
      else
        # Still has allocations on this node
        updated_node_allocations = Map.put(node_allocations, node, cleaned_req)
        new_process_allocations = Map.put(process_allocations, pid, updated_node_allocations)
        {new_process_allocations, monitors}
      end
    else
      :error ->
        # Process not tracked or no allocations on this node, nothing to clean up
        {process_allocations, monitors}
    end
  end

  defp cleanup_process_resources(pid, monitor_ref, nodes, process_allocations, monitors) do
    case Map.get(process_allocations, pid) do
      nil ->
        # Process had no allocations, just clean up the monitor
        new_monitors = Map.delete(monitors, monitor_ref)
        {nodes, process_allocations, new_monitors}

      node_allocations ->
        Logger.info(
          "Releasing resources for dead process #{inspect(pid)}: #{inspect(node_allocations)}"
        )

        # Release all resources allocated by this process
        new_nodes =
          Enum.reduce(node_allocations, nodes, fn {node, req}, acc_nodes ->
            case Map.get(acc_nodes, node) do
              nil ->
                acc_nodes

              %{used: used} = node_info ->
                new_used = update_used_resources(used, req, :remove)
                Map.put(acc_nodes, node, %{node_info | used: new_used})
            end
          end)

        # Clean up tracking
        new_process_allocations = Map.delete(process_allocations, pid)
        new_monitors = Map.delete(monitors, monitor_ref)

        {new_nodes, new_process_allocations, new_monitors}
    end
  end

  defp stop_monitoring_process(pid, process_allocations, monitors) do
    # Find and remove the monitor for this process
    {monitor_ref, _pid} = Enum.find(monitors, fn {_ref, p} -> p == pid end) || {nil, nil}
    if monitor_ref, do: Process.demonitor(monitor_ref)

    new_process_allocations = Map.delete(process_allocations, pid)
    new_monitors = Map.delete(monitors, monitor_ref)
    {new_process_allocations, new_monitors}
  end
end
