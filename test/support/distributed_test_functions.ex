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

  @doc """
  Sleeps for the specified duration and returns the value.
  Used for testing parallel execution timing for functions with no dependencies.
  """
  def sleep_and_return(value, sleep_ms) do
    Process.sleep(sleep_ms)
    value
  end

  @doc """
  Receives a dependency result, sleeps, and returns the given value.
  Used for testing parallel execution timing for functions with one dependency.
  """
  def sleep_with_dep_and_return(_dep_result, value, sleep_ms) do
    Process.sleep(sleep_ms)
    value
  end

  @doc """
  Sends a message to the test process when execution starts, waits for acknowledgment,
  then returns the value. Used for message-passing based parallelism tests.

  The function sends {:started, function_id, self()} so the test can send :continue
  directly to this process. This allows tests to verify parallel execution.
  """
  def notify_and_wait(test_pid, function_id, value) do
    send(test_pid, {:started, function_id, self()})

    receive do
      :continue -> :ok
    after
      10_000 -> raise "Timeout waiting for :continue message"
    end

    value
  end

  @doc """
  Same as notify_and_wait but for functions with a dependency argument.
  """
  def notify_and_wait_with_dep(_dep_result, test_pid, function_id, value) do
    send(test_pid, {:started, function_id, self()})

    receive do
      :continue -> :ok
    after
      10_000 -> raise "Timeout waiting for :continue message"
    end

    value
  end

  @doc """
  Sends a message when started, performs work, then sends completion.
  Used for verifying execution order and parallelism via message inspection.
  """
  def notify_start_and_complete(test_pid, function_id, value) do
    send(test_pid, {:started, function_id})
    # Small delay to ensure message ordering is detectable
    Process.sleep(10)
    send(test_pid, {:completed, function_id})
    value
  end

  @doc """
  Same as notify_start_and_complete but with a dependency argument.
  """
  def notify_start_and_complete_with_dep(_dep_result, test_pid, function_id, value) do
    send(test_pid, {:started, function_id})
    Process.sleep(10)
    send(test_pid, {:completed, function_id})
    value
  end

  @doc """
  Identity that sleeps so concurrent DAG resource claims overlap.
  """
  def slow_identity(value, sleep_ms \\ 150) do
    Process.sleep(sleep_ms)
    value
  end
end
