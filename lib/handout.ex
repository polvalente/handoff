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
end
