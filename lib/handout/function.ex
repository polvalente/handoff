defmodule Handout.Function do
  @moduledoc """
  Represents a function in a computation graph.

  Each function has:
  - an ID used for reference
  - a list of argument IDs that define dependencies
  - code to execute
  - a place to store results
  - an optional node assignment
  - an optional cost for resource planning
  - extra arguments that can be passed at execution time
  """

  @enforce_keys [:id, :args, :code]
  defstruct [
    # Unique identifier for the function
    :id,
    # List of function IDs whose results are needed as inputs
    :args,
    # Function to execute when dependencies are satisfied
    :code,
    # Storage for function results after execution
    :results,
    # Optional node assignment for distributed execution
    :node,
    # Optional resource cost for execution planning
    :cost,
    # Additional arguments provided at execution time
    extra_args: []
  ]

  @type t :: %__MODULE__{
          id: term(),
          args: [term()],
          code: function(),
          results: term() | nil,
          node: node() | nil,
          cost: map() | nil,
          extra_args: list()
        }
end
