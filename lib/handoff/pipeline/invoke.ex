defmodule Handoff.Pipeline.Invoke do
  @moduledoc false

  # Stream mode never retries: exceptions become Aggregator `{:item_error, ...}`
  # notifications (plus one-hop suppress). `Function.max_retries` is execute-only.

  @doc false
  defmacro safe(do: block) do
    quote do
      try do
        {:ok, unquote(block)}
      rescue
        e ->
          {:error, e}
      catch
        kind, reason ->
          {:error, {kind, reason}}
      end
    end
  end
end
