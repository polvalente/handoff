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

  Must be called before executing any DAGs.
  """
  def start(opts \\ []) do
    Handoff.Supervisor.start_link(opts)
  end

  @doc """
  Executes all functions in a DAG, respecting dependencies.

  ## Parameters
  - dag: The DAG to execute
  - opts: Optional execution settings
    - :allocation_strategy - Strategy for allocating functions to nodes
      (:first_available or :load_balanced, defaults to :first_available)

  ## Returns
  - {:ok, %{dag_id: dag_id, results: results_map}} with the DAG ID and a map of function IDs to results on success
  - {:error, reason} on failure
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
  - {:ok, %{dag_id: dag_id, results: results_map}} with the DAG ID and a map of function IDs to results on success
  - {:error, reason} on failure
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

  ## Parameters
  - node: The node to register
  - caps: Map of capabilities/resources the node provides

  ## Example
  ```
  Handoff.register_node(Node.self(), %{cpu: 4, memory: 8000})
  ```
  """
  def register_node(node, caps) do
    Handoff.SimpleResourceTracker.register(node, caps)
  end

  @doc """
  Discovers and registers nodes in the cluster with their capabilities.

  ## Returns
  - {:ok, discovered} with a map of node names to their capabilities
  """
  def discover_nodes do
    Handoff.DistributedExecutor.discover_nodes()
  end

  @doc """
  Registers the local node with its capabilities for distributed execution.

  ## Parameters
  - caps: Map of capabilities provided by this node

  ## Example
  ```
  Handoff.register_local_node(%{cpu: 8, memory: 16000})
  ```
  """
  def register_local_node(caps) do
    Handoff.DistributedExecutor.register_local_node(caps)
  end

  @doc """
  Checks if the specified node has the required resources available.

  ## Parameters
  - node: The node to check
  - req: Map of resource requirements to check

  ## Returns
  - true if resources are available
  - false otherwise

  ## Example
  ```
  Handoff.resources_available?(Node.self(), %{cpu: 2, memory: 4000})
  ```
  """
  def resources_available?(node, req) do
    Handoff.SimpleResourceTracker.available?(node, req)
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
  Explicitly broadcasts a result to all connected nodes for a specific DAG.
  Use this only when a result needs to be available everywhere.

  ## Parameters
  - dag_id: The ID of the DAG
  - function_id: The ID of the function
  - result: The result to broadcast
  """
  def broadcast_result(dag_id, function_id, result) do
    Handoff.DistributedResultStore.broadcast_result(dag_id, function_id, result)
  end

  @doc """
  Retrieves a result for a specific DAG, automatically fetching it from its origin node if necessary.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the result/argument to retrieve
  - timeout: Maximum time to wait in milliseconds, defaults to 5000

  ## Returns
  - {:ok, result} on success
  - {:error, :timeout} if the result is not available within the timeout
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
  - {:ok, value} if found locally
  - {:error, :not_found} if not found
  """
  def get_local_value(dag_id, id) do
    Handoff.ResultStore.get(dag_id, id)
  end

  @doc """
  Retrieves a value for a specific DAG, with automatic remote fetching if needed.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the value to retrieve
  - from_node: Optional specific node to fetch from

  ## Returns
  - {:ok, value} if found or successfully fetched
  - {:error, reason} if retrieval failed
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
  - dag_id: The ID of the DAG
  - data_id: The ID of the data to look up

  ## Returns
  - {:ok, node_id} if the data location is found
  - {:error, :not_found} if the data location is not registered
  """
  def lookup_data_location(dag_id, data_id) do
    Handoff.DataLocationRegistry.lookup(dag_id, data_id)
  end
end
