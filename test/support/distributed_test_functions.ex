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
  Notifies start then exits abnormally. Used for worker-crash tests.
  """
  def crash_after_notify(test_pid, function_id) do
    send(test_pid, {:started, function_id, self()})
    Process.sleep(10)
    raise "intentional worker crash"
  end

  @doc """
  Always raises after incrementing an agent counter. Used for retry-exhaustion tests.
  """
  def always_failing_function(_x, agent) do
    Agent.update(agent, fn state -> %{state | count: state.count + 1} end)
    raise "always fails"
  end

  @doc """
  Identity that sleeps so concurrent DAG resource claims overlap.
  """
  def slow_identity(value, sleep_ms \\ 150) do
    Process.sleep(sleep_ms)
    value
  end

  @doc """
  Returns `{Node.self(), value}` so streaming tests can assert stage placement.
  """
  def tag_with_node(value), do: {Node.self(), value}

  @doc """
  Doubles a value (named capture safe for remote stage invocation).
  """
  def double(value), do: value * 2

  @doc """
  Identity for remote streaming stages.
  """
  def identity(value), do: value
end
