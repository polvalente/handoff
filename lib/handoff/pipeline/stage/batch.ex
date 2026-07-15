defmodule Handoff.Pipeline.Stage.Batch do
  @moduledoc false

  @doc false
  def enabled?(%{batch_size: size, batch_timeout: timeout}) do
    size != nil or timeout != nil
  end

  @doc false
  def full?(%{batch_size: size}, buffer) when is_integer(size) do
    length(buffer) >= size
  end

  def full?(_function, _buffer), do: false

  @doc """
  Invokes batched `:code` with a list of arg-tuples and unbatches 1:1.

  `items` is `[{correlation_id, args_list}, ...]`. Returns
  `{[{correlation_id, result}, ...], new_worker_state}`.
  """
  def invoke_and_unbatch(function, worker_state, items) do
    arg_tuples = Enum.map(items, fn {_cid, args} -> List.to_tuple(args) end)
    expected = length(items)

    {results, new_state} = invoke(function, worker_state, arg_tuples)
    results = validate_results(results, expected)

    cids = Enum.map(items, fn {cid, _} -> cid end)
    {Enum.zip(cids, results), new_state}
  end

  defp invoke(%{init: nil} = function, _worker_state, arg_tuples) do
    {apply_code(function.code, [arg_tuples | function.extra_args]), nil}
  end

  defp invoke(function, worker_state, arg_tuples) do
    case apply_code(function.code, [worker_state, arg_tuples | function.extra_args]) do
      {results, new_state} ->
        {results, new_state}

      other ->
        raise "streaming batched :code with :init set must return {results_list, new_state}, got: #{inspect(other)}"
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
