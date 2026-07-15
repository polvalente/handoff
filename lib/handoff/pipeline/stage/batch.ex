defmodule Handoff.Pipeline.Stage.Batch do
  @moduledoc false

  @doc """
  Whether the buffer should flush now.

  - `batch_size` set → flush when length >= size
  - timeout-only → never size-full (timer flushes)
  - neither (batching off) → capacity 1, so any non-empty buffer is full
  """
  def full?(%{batch_size: size}, buffer) when is_integer(size) do
    length(buffer) >= size
  end

  def full?(%{batch_timeout: timeout}, _buffer) when is_integer(timeout), do: false

  def full?(_function, buffer), do: buffer != []

  @doc """
  Invokes `:code` for buffered items and returns 1:1 `{cid, result}` pairs.

  When `batch_size` and/or `batch_timeout` is set, `:code` receives a list of
  arg-tuples and must return a same-length list of results. Otherwise each item
  is invoked with the normal per-item calling convention (instant flush of
  size-1 batches).
  """
  def invoke_and_unbatch(
        %{batch_size: size, batch_timeout: timeout} = function,
        worker_state,
        items
      )
      when is_integer(size) or is_integer(timeout) do
    invoke_batched(function, worker_state, items)
  end

  def invoke_and_unbatch(function, worker_state, items) do
    invoke_each(function, worker_state, items)
  end

  defp invoke_batched(function, worker_state, items) do
    arg_tuples = Enum.map(items, fn {_cid, args} -> List.to_tuple(args) end)
    expected = length(items)

    {results, new_state} = invoke_batch_code(function, worker_state, arg_tuples)
    results = validate_results(results, expected)

    results = Enum.zip_with(items, results, fn {cid, _}, result -> {cid, result} end)
    {results, new_state}
  end

  defp invoke_each(function, worker_state, items) do
    Enum.map_reduce(items, worker_state, fn {cid, args}, ws ->
      {result, new_ws} = invoke(function, ws, args)
      {{cid, result}, new_ws}
    end)
  end

  defp invoke_batch_code(%{init: nil} = function, _worker_state, arg_tuples) do
    {apply_code(function.code, [arg_tuples | function.extra_args]), nil}
  end

  defp invoke_batch_code(function, worker_state, arg_tuples) do
    case apply_code(function.code, [worker_state, arg_tuples | function.extra_args]) do
      {results, new_state} ->
        {results, new_state}

      other ->
        raise "streaming batched :code with :init set must return {results_list, new_state}, got: #{inspect(other)}"
    end
  end

  defp invoke(%{init: nil} = function, _worker_state, args) do
    call_args =
      case function.argument_inclusion do
        :variadic -> args ++ function.extra_args
        :as_list -> [args | function.extra_args]
      end

    {apply_code(function.code, call_args), nil}
  end

  defp invoke(function, worker_state, args) do
    call_args =
      case function.argument_inclusion do
        :variadic -> [worker_state | args] ++ function.extra_args
        :as_list -> [worker_state, args | function.extra_args]
      end

    case apply_code(function.code, call_args) do
      {result, new_state} ->
        {result, new_state}

      other ->
        raise "streaming :code with :init set must return {result, new_state}, got: #{inspect(other)}"
    end
  end

  defp validate_results(results, expected) when is_list(results) do
    actual = length(results)

    if actual == expected do
      results
    else
      raise "streaming batched :code must return a list of #{expected} results, got #{actual}"
    end
  end

  defp validate_results(other, expected) do
    raise "streaming batched :code must return a list of #{expected} results, got: #{inspect(other)}"
  end

  defp apply_code(code, args) when is_function(code), do: apply(code, args)
  defp apply_code({module, fun}, args), do: apply(module, fun, args)
end
