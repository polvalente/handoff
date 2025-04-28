defmodule Handout.DynamicResourceTracker do
  @moduledoc """
  A resource tracker that supports dynamic updates to node capabilities at runtime.

  Extends the SimpleResourceTracker with the ability to:
  - Update node capabilities during runtime
  - Subscribe to capability change events
  - Automatically adjust to changing resource availability
  """

  @behaviour Handout.ResourceTracker

  use GenServer
  require Logger
  alias Handout.Telemetry
  alias Handout.Logger, as: HandoutLogger

  @table_name :handout_dynamic_resources

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

  @doc """
  Update the capabilities of a node at runtime.

  This allows for dynamic scaling of resources while the system is running.

  ## Parameters
  - `node`: The node to update
  - `caps`: New capability map or a function that transforms the existing capabilities

  ## Returns
  - `:ok` if successful
  - `{:error, :node_not_registered}` if the node isn't registered
  """
  def update_capabilities(node, caps_or_update_fn) do
    GenServer.call(__MODULE__, {:update_capabilities, node, caps_or_update_fn})
  end

  @doc """
  Subscribe to capability change events.

  ## Parameters
  - `subscriber`: PID or registered name to receive notifications
  - `nodes`: List of nodes to monitor or :all for all nodes

  ## Returns
  - `:ok`

  ## Notifications
  Subscribers will receive messages of the form:
  {:capability_change, node, old_caps, new_caps}
  """
  def subscribe(subscriber, nodes \\ :all) do
    GenServer.call(__MODULE__, {:subscribe, subscriber, nodes})
  end

  @doc """
  Unsubscribe from capability change events.

  ## Parameters
  - `subscriber`: PID or registered name to unsubscribe

  ## Returns
  - `:ok`
  """
  def unsubscribe(subscriber) do
    GenServer.call(__MODULE__, {:unsubscribe, subscriber})
  end

  @doc """
  Returns the capabilities of the local node.
  """
  def get_capabilities do
    GenServer.call(__MODULE__, :get_capabilities)
  end

  # Server callbacks

  @impl GenServer
  def init(_opts) do
    HandoutLogger.info("Starting dynamic resource tracker")
    table = :ets.new(@table_name, [:set, :protected, :named_table])

    {:ok,
     %{
       table: table,
       nodes: %{},
       # Map of subscriber => node list
       subscribers: %{}
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

    HandoutLogger.info("Registered node with capabilities",
      node: node,
      capabilities: inspect(caps)
    )

    # Emit telemetry event
    Telemetry.emit_event(
      Telemetry.resource_allocation(),
      %{system_time: System.system_time()},
      %{node: node, caps: caps, operation: :register}
    )

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
    start_time = System.system_time()

    # Emit request telemetry event
    Telemetry.emit_event(
      Telemetry.resource_request(),
      %{system_time: start_time},
      %{node: node, request: req}
    )

    case Map.get(state.nodes, node) do
      nil ->
        HandoutLogger.warning("Resource request failed: node not registered",
          node: node,
          request: inspect(req)
        )

        {:reply, {:error, :resources_unavailable}, state}

      %{full: full_caps, used: used} ->
        if check_resource_availability(full_caps, used, req) do
          # Update used resources
          new_used = update_used_resources(used, req, :add)

          nodes =
            Map.update!(state.nodes, node, fn node_info ->
              %{node_info | used: new_used}
            end)

          HandoutLogger.debug("Allocated resources",
            node: node,
            request: inspect(req)
          )

          # Emit allocation telemetry event
          Telemetry.emit_event(
            Telemetry.resource_allocation(),
            %{system_time: System.system_time()},
            %{node: node, request: req, operation: :allocate}
          )

          {:reply, :ok, %{state | nodes: nodes}}
        else
          HandoutLogger.warning("Resource request failed: insufficient resources",
            node: node,
            request: inspect(req),
            available: inspect(full_caps),
            used: inspect(used)
          )

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

        HandoutLogger.debug("Released resources",
          node: node,
          request: inspect(req)
        )

        # Emit release telemetry event
        Telemetry.emit_event(
          Telemetry.resource_release(),
          %{system_time: System.system_time()},
          %{node: node, request: req}
        )

        {:reply, :ok, %{state | nodes: nodes}}
    end
  end

  @impl GenServer
  def handle_call({:update_capabilities, node, caps_or_update_fn}, _from, state) do
    case Map.get(state.nodes, node) do
      nil ->
        HandoutLogger.warning("Cannot update capabilities: node not registered",
          node: node
        )

        {:reply, {:error, :node_not_registered}, state}

      node_info ->
        old_caps = node_info.full

        # Apply the update
        new_caps =
          if is_function(caps_or_update_fn) do
            caps_or_update_fn.(old_caps)
          else
            caps_or_update_fn
          end

        # Create updated node info
        updated_node_info = %{node_info | full: new_caps}

        # Update state
        nodes = Map.put(state.nodes, node, updated_node_info)

        # Notify subscribers
        notify_subscribers(state.subscribers, node, old_caps, new_caps)

        HandoutLogger.info("Updated node capabilities",
          node: node,
          old_caps: inspect(old_caps),
          new_caps: inspect(new_caps)
        )

        # Emit telemetry event for capability update
        Telemetry.emit_event(
          Telemetry.resource_allocation(),
          %{system_time: System.system_time()},
          %{node: node, old_caps: old_caps, new_caps: new_caps, operation: :update}
        )

        {:reply, :ok, %{state | nodes: nodes}}
    end
  end

  @impl GenServer
  def handle_call({:subscribe, subscriber, nodes}, _from, state) do
    subscribers = Map.put(state.subscribers, subscriber, nodes)
    {:reply, :ok, %{state | subscribers: subscribers}}
  end

  @impl GenServer
  def handle_call({:unsubscribe, subscriber}, _from, state) do
    subscribers = Map.delete(state.subscribers, subscriber)
    {:reply, :ok, %{state | subscribers: subscribers}}
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

  defp notify_subscribers(subscribers, changed_node, old_caps, new_caps) do
    Enum.each(subscribers, fn {subscriber, nodes} ->
      if nodes == :all or changed_node in List.wrap(nodes) do
        send(subscriber, {:capability_change, changed_node, old_caps, new_caps})
      end
    end)
  end
end
