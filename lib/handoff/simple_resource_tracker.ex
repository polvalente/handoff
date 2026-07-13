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

  @doc """
  Atomically allocate functions onto nodes using *remaining* capacity (`full - used`)
  and claim those resources for `claimant_pid` (typically the execute Task).

  Concurrent callers serialize on this GenServer, so overflow spills to the next
  node instead of over-committing the first one from stale full-capacity snapshots.
  """
  @spec allocate_and_claim([Handoff.Function.t()], [node()], pid()) ::
          {:ok, %{term() => node()}} | {:error, :resources_unavailable | term()}
  def allocate_and_claim(functions, nodes, claimant_pid \\ self())
      when is_list(functions) and is_list(nodes) and is_pid(claimant_pid) do
    GenServer.call(__MODULE__, {:allocate_and_claim, functions, nodes, claimant_pid})
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
  def handle_call({:allocate_and_claim, functions, nodes, claimant_pid}, _from, state) do
    # Refresh full caps from each node (remote get_capabilities) while preserving
    # locally tracked `used` so concurrent claims serialize correctly.
    state = sync_full_capabilities(state, nodes)

    available_caps =
      Map.new(nodes, fn node ->
        case Map.get(state.nodes, node) do
          %{full: full, used: used} ->
            {node, remaining_caps(full, used)}

          _ ->
            {node, %{}}
        end
      end)

    try do
      assignments = Handoff.SimpleAllocator.allocate(functions, available_caps)

      case claim_assignments(functions, assignments, claimant_pid, state) do
        {:ok, new_state} ->
          {:reply, {:ok, assignments}, new_state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    rescue
      e in [Handoff.Allocator.AllocationError] ->
        {:reply, {:error, {:allocation_error, e.message}}, state}
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

  defp sync_full_capabilities(state, nodes) do
    nodes =
      Enum.reduce(nodes, state.nodes, fn node, acc ->
        full = fetch_full_capabilities(node, acc)
        used = get_in(acc, [node, Access.key(:used)]) || %{}
        Map.put(acc, node, %{full: full, used: used})
      end)

    %{state | nodes: nodes}
  end

  defp fetch_full_capabilities(node, local_nodes) when node == node() do
    case Map.get(local_nodes, node) do
      %{full: full} -> full
      _ -> %{}
    end
  end

  defp fetch_full_capabilities(node, local_nodes) do
    case :rpc.call(node, __MODULE__, :get_capabilities, []) do
      caps when is_map(caps) and map_size(caps) > 0 ->
        caps

      _ ->
        case Map.get(local_nodes, node) do
          %{full: full} -> full
          _ -> %{}
        end
    end
  end

  defp remaining_caps(full, used) do
    full
    |> Map.keys()
    |> Enum.concat(Map.keys(used))
    |> Enum.uniq()
    |> Map.new(fn key ->
      {key, Map.get(full, key, 0) - Map.get(used, key, 0)}
    end)
  end

  defp claim_assignments(functions, assignments, claimant_pid, state) do
    functions_by_id = Map.new(functions, &{&1.id, &1})

    # Validate all claims up-front so we don't create monitors / mutate state and then discard it.
    can_claim? =
      Enum.all?(assignments, fn {function_id, node} ->
        function = Map.fetch!(functions_by_id, function_id)
        cost = function.cost || %{}

        if cost == %{} do
          true
        else
          case Map.get(state.nodes, node) do
            %{full: full, used: used} -> check_resource_availability(full, used, cost)
            _ -> false
          end
        end
      end)

    if can_claim? do
      Enum.reduce_while(assignments, {:ok, state}, fn {function_id, node}, {:ok, acc_state} ->
        function = Map.fetch!(functions_by_id, function_id)
        cost = function.cost || %{}

        if cost == %{} do
          {:cont, {:ok, acc_state}}
        else
          case claim_on_state(acc_state, node, cost, claimant_pid) do
            {:ok, new_state} -> {:cont, {:ok, new_state}}
            {:error, _} = error -> {:halt, error}
          end
        end
      end)
    else
      {:error, :resources_unavailable}
    end
  end

  defp claim_on_state(state, node, req, claimant_pid) do
    case Map.get(state.nodes, node) do
      nil ->
        {:error, :resources_unavailable}

      %{full: full_caps, used: used} ->
        if check_resource_availability(full_caps, used, req) do
          new_used = update_used_resources(used, req, :add)

          nodes =
            Map.update!(state.nodes, node, fn node_info ->
              %{node_info | used: new_used}
            end)

          {new_process_allocations, new_monitors} =
            track_process_allocation(
              claimant_pid,
              node,
              req,
              state.process_allocations,
              state.monitors
            )

          Logger.debug(
            "Claimed resources on node #{inspect(node)} for #{inspect(claimant_pid)}: #{inspect(req)}"
          )

          {:ok,
           %{
             state
             | nodes: nodes,
               process_allocations: new_process_allocations,
               monitors: new_monitors
           }}
        else
          {:error, :resources_unavailable}
        end
    end
  end

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
    if Map.has_key?(process_allocations, pid) do
      update_existing_allocation(pid, node, req, process_allocations, monitors)
    else
      start_monitoring_process(pid, node, req, process_allocations, monitors)
    end
  end

  defp update_existing_allocation(pid, node, req, process_allocations, monitors) do
    updated_allocations =
      Map.update!(process_allocations, pid, fn node_allocations ->
        Map.update(node_allocations, node, req, &merge_resource_requirements(&1, req))
      end)

    {updated_allocations, monitors}
  end

  defp start_monitoring_process(pid, node, req, process_allocations, monitors) do
    monitor_ref = Process.monitor(pid)
    new_process_allocations = Map.put(process_allocations, pid, %{node => req})
    new_monitors = Map.put(monitors, monitor_ref, pid)
    {new_process_allocations, new_monitors}
  end

  defp merge_resource_requirements(existing_req, new_req) do
    Map.merge(existing_req, new_req, fn _key, v1, v2 -> v1 + v2 end)
  end

  defp untrack_process_allocation(pid, node, req, process_allocations, monitors) do
    with {:ok, node_allocations} <- Map.fetch(process_allocations, pid),
         {:ok, existing_req} <- Map.fetch(node_allocations, node) do
      # Subtract the released resources
      new_req = Map.merge(existing_req, req, fn _key, v1, v2 -> max(0, v1 - v2) end)

      # Remove zero allocations
      cleaned_req = new_req |> Enum.reject(fn {_resource, amount} -> amount == 0 end) |> Map.new()

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
