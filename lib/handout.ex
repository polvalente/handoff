defmodule Handout do
  @moduledoc """
  Handout is a library for building and executing Directed Acyclic Graphs (DAGs) of functions.

  It provides tools for defining computation graphs, managing resources, and executing
  the graphs in a distributed environment.
  """

  @doc """
  Creates a new DAG instance.
  """
  def new do
    Handout.DAG.new()
  end

  @doc """
  Starts the Handout supervision tree.

  Must be called before executing any DAGs.
  """
  def start do
    Handout.Supervisor.start_link([])
  end

  @doc """
  Executes all functions in a DAG, respecting dependencies.

  ## Parameters
  - dag: The DAG to execute
  - opts: Optional execution settings
    - :allocation_strategy - Strategy for allocating functions to nodes
      (:first_available or :load_balanced, defaults to :first_available)

  ## Returns
  - {:ok, results} with a map of function IDs to results on success
  - {:error, reason} on failure
  """
  def execute(dag, opts \\ []) do
    Handout.Executor.execute(dag, opts)
  end

  @doc """
  Registers a node with its resource capabilities.

  ## Parameters
  - node: The node to register
  - caps: Map of capabilities/resources the node provides

  ## Example
  ```
  Handout.register_node(Node.self(), %{cpu: 4, memory: 8000})
  ```
  """
  def register_node(node, caps) do
    Handout.SimpleResourceTracker.register(node, caps)
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
  Handout.resources_available?(Node.self(), %{cpu: 2, memory: 4000})
  ```
  """
  def resources_available?(node, req) do
    Handout.SimpleResourceTracker.available?(node, req)
  end
end
