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

  def counting_identity_function(value, counter_agent) do
    Agent.update(counter_agent, fn state -> %{count: state.count + 1} end)
    value
  end

  def serialize(term, source_node, target_node) do
    if source_node == target_node do
      term
    else
      :erlang.term_to_binary(term)
    end
  end

  def deserialize(term, source_node, target_node) do
    if source_node == target_node do
      term
    else
      :erlang.binary_to_term(term)
    end
  end

  def elem_with_nodes(tuple, _source_node, _target_node, index) do
    elem(tuple, index)
  end
end
