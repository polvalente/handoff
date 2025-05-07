defmodule Handoff.DistributedTestFunctions do
  @moduledoc """
  Helper functions for distributed tests that can be safely sent between nodes.
  Each function returns a list containing its arguments, making it easy to verify
  that the function was called with the correct arguments.
  """

  def f(x), do: [x]
  def g(x, y), do: [x, y]
  def h(x, y, z), do: [x, y, z]
end
