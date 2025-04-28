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

  ## Returns
  - {:ok, results} with a map of function IDs to results on success
  - {:error, reason} on failure
  """
  def execute(dag, opts \\ []) do
    Handout.Executor.execute(dag, opts)
  end
end
