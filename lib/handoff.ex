defmodule Handoff do
  @moduledoc """
  Handoff is a library for building and executing Directed Acyclic Graphs (DAGs) of functions.

  It provides tools for defining computation graphs, managing resources, and executing
  the graphs in a distributed environment.
  """

  @doc """
  Creates a new DAG instance.
  """
  def new do
    Handoff.DAG.new()
  end

  @doc """
  Starts the Handoff supervision tree.

  The resource tracker can be configured via application config:
  ```
  config :handoff, resource_tracker: MyResourceTracker
  ```

  Or overridden via opts for backward compatibility:
  ```
  Handoff.start(resource_tracker: MyResourceTracker)
  ```

  Must be called before executing any DAGs.
  Returns the supervisor pid and the resource tracker pid or name.
  """
  def start(opts \\ []) do
    resource_tracker =
      Keyword.get(opts, :resource_tracker) ||
        Application.get_env(:handoff, :resource_tracker, Handoff.SimpleResourceTracker)

    tracker =
      case resource_tracker do
        mod when is_atom(mod) ->
          # Start the tracker if it's a module
          {:ok, pid} = mod.start_link([])
          pid

        pid when is_pid(pid) or is_atom(pid) ->
          pid
      end

    {:ok, sup_pid} = Handoff.Supervisor.start_link(Keyword.put(opts, :resource_tracker, tracker))
    {:ok, sup_pid, tracker}
  end

  @doc """
  Executes all functions in a DAG, respecting dependencies.

  ## Parameters
  - dag: The DAG to execute
  - opts: Optional execution settings
    - :allocation_strategy - Strategy for allocating functions to nodes
      (:first_available or :load_balanced, defaults to :first_available)

  ## Returns
  - `{:ok, %{dag_id: dag_id, results: results_map}}` with the DAG ID and a map of function IDs to results on success
  - `{:error, reason}` on failure
  """
  def execute(dag, opts \\ []) do
    # Use DistributedExecutor, which handles local execution if no other nodes are present.
    Handoff.DistributedExecutor.execute(dag, opts)
  end

  @doc """
  Executes all functions in a DAG across multiple nodes, respecting dependencies.

  ## Parameters
  - dag: The DAG to execute
  - opts: Optional execution settings
    - :allocation_strategy - Strategy for allocating functions to nodes
      (:first_available or :load_balanced, defaults to :first_available)
    - :max_retries - Maximum number of times to retry failed functions (default: 3)

  ## Returns
  - `{:ok, %{dag_id: dag_id, results: results_map}}` with the DAG ID and a map of function IDs to results on success
  - `{:error, reason}` on failure
  """
  def execute_distributed(dag, opts \\ []) do
    Handoff.DistributedExecutor.execute(dag, opts)
  end

  @doc """
  Executes all functions in a DAG strictly on the local node, bypassing resource allocation
  and ensuring all functions have `node` set to `Node.self()` and `cost` set to `nil`.

  This is useful for ensuring local execution regardless of global configuration or function definitions.

  ## Parameters
  - dag: The DAG to execute
  - opts: Optional execution settings (passed to `Handoff.DistributedExecutor`)

  ## Returns
  - Same as `Handoff.execute/2`.
  """
  def execute_local(dag, opts \\ []) do
    # TODO: use simpler topo-sort + reduce strategy for executing the graph locally.
    dag =
      update_in(dag, [Access.key(:functions), Access.all()], fn {id, func} ->
        modified_func = %{func | node: Node.self(), cost: nil}
        {id, modified_func}
      end)

    Handoff.DistributedExecutor.execute(dag, opts)
  end

  @doc """
  Registers a node with its resource capabilities.

  For local nodes, registers directly with the local resource tracker.
  For remote nodes, makes an RPC call to register on the remote node.

  ## Parameters
  - tracker: The resource tracker pid or name (optional, defaults to application config)
  - node: The node to register
  - caps: Map of capabilities/resources the node provides

  ## Examples
  ```
  # Using default tracker from application config
  Handoff.register_node(Node.self(), %{cpu: 4, memory: 8000})

  # Using specific tracker
  Handoff.register_node(tracker, Node.self(), %{cpu: 4, memory: 8000})
  ```
  """
  def register_node(node, caps) do
    register_node(get_resource_tracker(), node, caps)
  end

  def register_node(tracker, node, caps) do
    if node == Node.self() do
      # Local node: register directly with the resource tracker
      # The DistributedExecutor will discover this node through its discovery process
      tracker.register(node, caps)
    else
      # Remote node: use RPC to register on the remote node
      case :rpc.call(node, tracker, :register, [node, caps]) do
        {:badrpc, reason} ->
          {:error, {:rpc_failed, reason}}

        result ->
          result
      end
    end
  end

  @doc """
  Discovers and registers nodes in the cluster with their capabilities.

  ## Returns
  - `{:ok, discovered}` with a map of node names to their capabilities
  """
  def discover_nodes do
    Handoff.DistributedExecutor.discover_nodes()
  end

  @doc """
  Checks if the specified node has the required resources available.

  For local nodes, checks directly with the local resource tracker.
  For remote nodes, makes an RPC call to check on the remote node directly.

  ## Parameters
  - tracker: The resource tracker pid or name (optional, defaults to application config)
  - node: The node to check
  - req: Map of resource requirements to check

  ## Returns
  - true if resources are available
  - false otherwise

  ## Examples
  ```
  # Using default tracker from application config
  Handoff.resources_available?(Node.self(), %{cpu: 2, memory: 4000})

  # Using specific tracker
  Handoff.resources_available?(Handoff.SimpleResourceTracker, Node.self(), %{cpu: 2, memory: 4000})
  ```
  """
  def resources_available?(node, req) do
    resources_available?(get_resource_tracker(), node, req)
  end

  def resources_available?(tracker, node, req) do
    if node == Node.self() do
      # Local node: check directly
      tracker.available?(node, req)
    else
      # Remote node: use RPC to check on the source node
      case :rpc.call(node, tracker, :available?, [node, req]) do
        {:badrpc, _reason} ->
          false

        result ->
          result
      end
    end
  end

  @doc """
  Stores a function result locally on the origin node for a specific DAG and registers its location.
  The result is stored only on the node where it was produced, not broadcast.

  ## Parameters
  - dag_id: The ID of the DAG
  - function_id: The ID of the function
  - result: The result to store
  - origin_node: The node where the result was produced (defaults to current node)
  """
  def store_result(dag_id, function_id, result, origin_node \\ Node.self()) do
    Handoff.DistributedResultStore.store_distributed(dag_id, function_id, result, origin_node)
  end

  @doc """
  Retrieves a result for a specific DAG, automatically fetching it from its origin node if necessary.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the result/argument to retrieve
  - timeout: Maximum time to wait in milliseconds, defaults to 5000

  ## Returns
  - `{:ok, result}` on success
  - `{:error, :timeout}` if the result is not available within the timeout
  """
  def get_result(dag_id, id, timeout \\ 5000) do
    Handoff.DistributedResultStore.get_with_timeout(dag_id, id, timeout)
  end

  @doc """
  Directly stores a value in the local store for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the value
  - value: The value to store
  """
  def store_value(dag_id, id, value) do
    Handoff.ResultStore.store(dag_id, id, value)
  end

  @doc """
  Retrieves a value from the local store only for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the value to retrieve

  ## Returns
  - `{:ok, value}` if found locally
  - `{:error, :not_found}` if not found
  """
  def get_local_value(dag_id, id) do
    Handoff.ResultStore.get(dag_id, id)
  end

  @doc """
  Retrieves a value for a specific DAG, with automatic remote fetching if needed.

  ## Parameters
  - `dag_id`: The ID of the DAG
  - `id`: The ID of the value to retrieve
  - `from_node`: Optional specific node to fetch from

  ## Returns
  - `{:ok, value}` if found or successfully fetched
  - `{:error, reason}` if retrieval failed
  """
  def get_value(dag_id, id, from_node \\ nil) do
    Handoff.ResultStore.get_with_fetch(dag_id, id, from_node)
  end

  @doc """
  Registers the location of a data item (argument or result) for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - data_id: The ID of the data
  - node_id: The node where the data is stored
  """
  def register_data_location(dag_id, data_id, node_id) do
    Handoff.DataLocationRegistry.register(dag_id, data_id, node_id)
  end

  @doc """
  Looks up where a data item (argument or result) is stored for a specific DAG.

  ## Parameters
  - `dag_id`: The ID of the DAG
  - `data_id`: The ID of the data to look up

  ## Returns
  - `{:ok, node_id}` if the data location is found
  - `{:error, :not_found}` if the data location is not registered
  """
  def lookup_data_location(dag_id, data_id) do
    Handoff.DataLocationRegistry.lookup(dag_id, data_id)
  end

  defp get_resource_tracker do
    Application.get_env(:handoff, :resource_tracker, Handoff.SimpleResourceTracker)
  end
end
