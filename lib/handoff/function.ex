defmodule Handoff.Function do
  @moduledoc """
  Represents a function in a computation graph.

  ## Structure

  Each function in a Handoff DAG has these key components:

  * `:id` - A unique identifier for the function (any term, typically an atom)
  * `:args` - A list of other function IDs whose results this function depends on
  * `:code` - The actual function to execute, receives results of dependencies as input.
    May be `nil` only for `type: :input` nodes (values supplied via the streaming pipeline).
  * `:results` - Storage for function output after execution
  * `:node` - Optional node assignment for distributed execution
  * `:cost` - Optional resource requirements map (e.g., %{cpu: 2, memory: 1000})
  * `:extra_args` - Additional arguments provided at execution time
  * `:type` - Type of the function: `:regular`, `:inline`, or `:input`
    * `:regular` - Normal node (allocated/executed as its own stage or task)
    * `:inline` - Absorbed into dependents: never scheduled alone; re-evaluated
      whenever a dependent needs it (batch and streaming). Must not set `:node`
      or `:init`.
    * `:input` - Streaming source placeholder (`code: nil`); values via `push/2`
  * `:max_retries` - Optional per-function retry override (`nil` inherits the
    execute-level `:max_retries`, default 3). Use `0` for no retries.
  * `:argument_inclusion` - One of :variadic or :as_list (defaults to :variadic)
    * `:variadic` - Pass the list of N arguments as the first N arguments to the function
    * `:as_list` - Pass arguments as a list in the first argument to the function
  * `:init` - Optional one-time setup for streaming (`Handoff.stream/2`). Must be an
    MFA `{module, fun, args}` or a named `&Module.function/arity` capture — never an
    anonymous function. Run once per stage worker; ignored by `Handoff.execute/2`.
    Not allowed on `:inline` or `:input` functions.
  * `:parallelism` - Number of concurrent stage-worker replicas when streaming
    (default `1`). Ignored by `Handoff.execute/2`.
  * `:batch_size` / `:batch_timeout` - Optional batching controls for streaming stages.
    When either is set, ready items are accumulated until the buffer reaches
    `:batch_size` or `:batch_timeout` milliseconds elapse since the first buffered
    item (whichever comes first). Ignored by `Handoff.execute/2`.

  ## Streaming calling convention

  When `:init` is set and the function runs via `Handoff.stream/2`, `:code` receives
  the worker state as its first argument and must return `{result, new_state}`:

      code.(state, arg1, ..., argN)  # or code.(state, [args]) for `:as_list`

  When `:init` is `nil`, `:code` keeps the stateless signature used by `Handoff.execute/2`.

  ### Batched streaming

  When `:batch_size` and/or `:batch_timeout` is set, `:code` is invoked once per
  flush with a **list of arg-tuples** (one tuple per buffered item, fields in
  dependency order). It must return a **same-length list of results**, which are
  unbatched 1:1 onto the original correlation ids. With `:init`, the return is
  `{results_list, new_state}` (state advances once per flush):

      code.([{arg1, ...}, ...])              # no :init
      code.(state, [{arg1, ...}, ...])       # with :init → {[r1, ...], new_state}

  A non-list or wrong-length return raises in the stage/worker (contract
  violation) rather than silently corrupting downstream correlation ids.

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

  # An input placeholder for streaming pipelines (value supplied per item via push)
  %Handoff.Function{
    id: :source,
    args: [],
    code: nil,
    type: :input
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
    # Function to execute when dependencies are satisfied (`nil` only for `:input`)
    :code,
    # Storage for function results after execution
    :results,
    # Optional node assignment for distributed execution
    :node,
    # Optional resource cost for execution planning
    :cost,
    # Additional arguments provided at execution time
    extra_args: [],
    # Type of the function: :regular, :inline, or :input
    type: :regular,
    # Optional per-function retry override; nil inherits execute-level default
    max_retries: nil,
    # How to arguments are passed into :code
    argument_inclusion: :variadic,
    # One-time setup for streaming stage workers (MFA or named capture)
    init: nil,
    # Concurrent stage-worker replicas for streaming (ignored by execute/2)
    parallelism: 1,
    # Optional streaming batch size
    batch_size: nil,
    # Optional streaming batch timeout
    batch_timeout: nil
  ]

  @type t :: %__MODULE__{
          id: term(),
          args: [term() | Handoff.Function.Argument.t()],
          code: function() | {module(), atom()} | nil,
          results: term() | nil,
          node: node() | nil,
          cost: map() | nil,
          extra_args: list(),
          type: :regular | :inline | :input,
          max_retries: non_neg_integer() | nil,
          argument_inclusion: :variadic | :as_list,
          init: nil | {module(), atom(), [term()]} | function(),
          parallelism: pos_integer(),
          batch_size: pos_integer() | nil,
          batch_timeout: non_neg_integer() | nil
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
    deserialization_fn: nil,
    # How to include the argument in the function call
    argument_inclusion: :variadic
  ]

  @type t :: %__MODULE__{
          id: term(),
          serialization_fn: {module(), atom(), [term()]} | nil,
          deserialization_fn: {module(), atom(), [term()]} | nil,
          argument_inclusion: :variadic | :as_list
        }
end
