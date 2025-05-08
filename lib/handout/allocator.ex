defmodule Handoff.Allocator do
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
              functions :: [Handoff.Function.t()],
              caps :: %{node() => map()}
            ) :: %{term() => node()}

  defmodule AllocationError do
    @moduledoc false
    defexception [:message]

    def exception(message) do
      %AllocationError{message: message}
    end
  end
end
