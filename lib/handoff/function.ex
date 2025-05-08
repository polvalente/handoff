defmodule Handoff.Function do
  @moduledoc """
  Represents a function in a computation graph.

  ## Structure

  Each function in a Handoff DAG has these key components:

  * `:id` - A unique identifier for the function (any term, typically an atom)
  * `:args` - A list of other function IDs whose results this function depends on
  * `:code` - The actual function to execute, receives results of dependencies as input
  * `:results` - Storage for function output after execution
  * `:node` - Optional node assignment for distributed execution
  * `:cost` - Optional resource requirements map (e.g., %{cpu: 2, memory: 1000})
  * `:extra_args` - Additional arguments provided at execution time
  * `:type` - Type of the function, :regular or :inline

  ## Examples

  ```elixir
  # A simple computation function with no dependencies
  %Handoff.Function{
    id: :generate_data,
    args: [],
    code: fn -> Enum.random(1..100) end
  }

  # A function that depends on another function's result
  %Handoff.Function{
    id: :process_data,
    args: [:generate_data],
    code: fn %{generate_data: value} -> value * 2 end
  }

  # A function with resource requirements for distributed execution
  %Handoff.Function{
    id: :heavy_computation,
    args: [:process_data],
    code: fn %{process_data: value} -> complex_algorithm(value) end,
    cost: %{cpu: 4, memory: 8000}
  }
  ```

  ## Resource Costs

  The `:cost` field allows you to specify resource requirements for distributed execution:

  * `:cpu` - Number of CPU cores required
  * `:memory` - Memory in MB required
  * `:gpu` - GPU units required (if applicable)
  * Custom resource types can also be defined

  These costs are used by allocators to determine which nodes can run which functions.
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
    extra_args: [],
    # Type of the function, :regular or :inline
    type: :regular
  ]

  @type t :: %__MODULE__{
          id: term(),
          args: [term()],
          code: function(),
          results: term() | nil,
          node: node() | nil,
          cost: map() | nil,
          extra_args: list(),
          type: :regular | :inline
        }
end
