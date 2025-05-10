defmodule Handoff.InternalOps do
  @moduledoc false

  @doc """
  Identity function that expects node information but ignores it.
  Used as a default if user doesn't provide a ser/deser function.
  Signature: data, source_node, target_node, any_static_args...
  """
  def identity_with_nodes(data, _source_node, _target_node, _extra_args \\ []) do
    data
  end
end
