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
    code: &Enum.random/1,
    extra_args: [1..100]
  }

  # A function that depends on another function's result.
  # The result of :generate_data (e.g., an integer) will be passed as the first argument
  # to &*/2. The second argument for the multiplication (2) comes from extra_args.
  %Handoff.Function{
    id: :process_data,
    args: [:generate_data],
    code: &*/2,
    extra_args: [2]
    # If :generate_data produces X, this executes X * 2.
  }

  # A function with resource requirements for distributed execution.
  # The result of :process_data will be passed as the first argument to IO.inspect/2.
  %Handoff.Function{
    id: :inspect_result,
    args: [:process_data],
    code: &IO.inspect/2,
    extra_args: [label: "Inspect result"],
    cost: %{cpu: 1, memory: 500} # Adjusted cost for a simple inspect
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
          args: [term() | Handoff.Function.Argument.t()],
          code: function(),
          results: term() | nil,
          node: node() | nil,
          cost: map() | nil,
          extra_args: list(),
          type: :regular | :inline
        }
end

defmodule Handoff.Function.Argument do
  @moduledoc """
  Defines a function argument with optional transformations.
  """
  @enforce_keys [:id]
  defstruct [
    # Atom: ID of the producer function.
    :id,
    # User-provided MFA `{module, function, static_arg}`.
    serialization_fn: nil,
    # Executed on the producer's node.
    # Receives: `data, source_node, target_node, static_arg`
    # User-provided MFA `{module, function, static_arg}`.
    deserialization_fn: nil
    # Executed on the consumer's node.
    # Receives: `data, source_node, target_node, static_arg`
  ]

  @type t :: %__MODULE__{
          id: term(),
          serialization_fn: {module(), atom(), [term()]} | nil,
          deserialization_fn: {module(), atom(), [term()]} | nil
        }
end
