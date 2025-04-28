defmodule Handout.SimpleResourceTracker do
  @moduledoc """
  A simple implementation of the ResourceTracker behavior that tracks
  resources using an ETS table.
  """

  @behaviour Handout.ResourceTracker

  use GenServer
  require Logger

  @table_name :handout_resources

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Handout.ResourceTracker
  def register(node, caps) do
    GenServer.call(__MODULE__, {:register, node, caps})
  end

  @impl Handout.ResourceTracker
  def available?(node, req) do
    GenServer.call(__MODULE__, {:available?, node, req})
  end

  @impl Handout.ResourceTracker
  def request(node, req) do
    GenServer.call(__MODULE__, {:request, node, req})
  end

  @impl Handout.ResourceTracker
  def release(node, req) do
    GenServer.call(__MODULE__, {:release, node, req})
  end

  # Server callbacks

  @impl GenServer
  def init(_opts) do
    table = :ets.new(@table_name, [:set, :protected, :named_table])
    {:ok, %{table: table, nodes: %{}}}
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
  def handle_call({:request, node, req}, _from, state) do
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

          Logger.debug("Allocated resources on node #{inspect(node)}: #{inspect(req)}")

          {:reply, :ok, %{state | nodes: nodes}}
        else
          {:reply, {:error, :resources_unavailable}, state}
        end
    end
  end

  @impl GenServer
  def handle_call({:release, node, req}, _from, state) do
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

        Logger.debug("Released resources on node #{inspect(node)}: #{inspect(req)}")

        {:reply, :ok, %{state | nodes: nodes}}
    end
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
end
