defmodule Handout.Allocator do
  @moduledoc """
  Behavior for allocating functions to nodes based on resource requirements.

  This module is responsible for determining which nodes should execute which
  functions, considering their resource requirements and available capabilities.
  """

  @doc """
  Allocate functions to nodes based on resource requirements and node capabilities.

  ## Parameters
  - `functions`: List of functions to allocate
  - `caps`: Map of node capabilities in the format %{node() => capabilities_map}

  ## Returns
  A map with function IDs as keys and node assignments as values.
  """
  @callback allocate(
              functions :: [Handout.Function.t()],
              caps :: %{node() => map()}
            ) :: %{term() => node()}
end
