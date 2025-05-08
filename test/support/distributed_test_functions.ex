defmodule Handoff.DistributedTestFunctions do
  @moduledoc """
  Helper functions for distributed tests that can be safely sent between nodes.
  Each function returns a list containing its arguments, making it easy to verify
  that the function was called with the correct arguments.
  """

  def f(x), do: [x]
  def g(x, y), do: [x, y]
  def h(x, y, z), do: [x, y, z]

  def raise_error(value, message) do
    raise message <> ", value: #{inspect(value)}"
  end

  def failing_function(x, agent) do
    count = Agent.get(agent, fn state -> state.count end)
    Agent.update(agent, fn state -> %{state | count: state.count + 1} end)

    if count == 0 do
      # First call fails
      raise "Simulated failure for testing"
    else
      # Second call succeeds
      x * 2
    end
  end
end
