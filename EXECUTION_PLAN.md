# Handoff Enhancement: Tuple Extraction and Conditional SerDe Execution Plan

## 1. Goals

1. Allow users to specify that only a specific element of a tuple result from a producer function should be used as a dependency. This extraction should occur on the producer's node.
2. Allow users to define custom serialization and deserialization functions for data passing between functions.
3. These user-defined serialization and deserialization functions should *always* execute.
4. These functions must receive information about the source node (where the data was produced/serialized) and the target node (where the data will be consumed/deserialized) to allow conditional logic within them (e.g., skip actual serialization if source and target nodes are the same).
5. These transformations (extraction, serialization, deserialization) should be represented as distinct (potentially synthetic) functions in the DAG to leverage existing execution and scheduling, with specific colocation requirements.

## 2. Proposed Core Changes

### 2.1. `Handoff.Function.Argument` Struct

A new struct will define how a function depends on the output of another, allowing for transformations.

```elixir
defmodule Handoff.Function.Argument do
  @moduledoc """
  Defines a function argument with optional transformations.
  """
  @enforce_keys [:id]
  defstruct [
    :id,                 # Atom: ID of the producer function.
    serialization_fn: nil, # User-provided MFA `{module, function, static_arg}`.
                         # Executed on the producer's node.
                         # Receives: `data, source_node, target_node, static_arg`
    deserialization_fn: nil # User-provided MFA `{module, function, static_arg}`.
                         # Executed on the consumer's node.
                         # Receives: `data, source_node, target_node, static_arg`
  ]

  @type t :: %__MODULE__{
    id: term(),
    serialization_fn: {module(), atom(), [term()]} | nil,
    deserialization_fn: {module(), atom(), [term()]} | nil
  }
end
```

If `serialization_fn` or `deserialization_fn` are `nil`, they default to an identity operation (pass-through without extra node/function call if possible, or a simple identity function call if a node is synthesized). For simplicity in the initial implementation, we might always synthesize nodes and use `Elixir.Function.identity/1` if a user function is nil.

### 2.2. `Handoff.Function` Struct Modification

The `args` field in `Handoff.Function` will be updated to accept either a term (existing behavior for direct dependency) or the new `Handoff.Function.Argument.t()` struct.

```elixir
# In Handoff.Function
# ...
@type t :: %__MODULE__{
          id: term(),
          args: [term() | Handoff.Function.Argument.t()], # MODIFIED
          code: function(),
          # ... other fields
        }
```

### 2.3. DAG Rewriting in `Handoff.DAG.add_function/2`

This function will be responsible for "expanding" `Handoff.Function.Argument` definitions into a sequence of synthetic `Handoff.Function` nodes.

**For each `arg_spec :: Handoff.Function.Argument.t()` in a consuming function `F`'s `args`:**

Let `consuming_function_id = F.id`.
Let `original_producer_id = arg_spec.id`.
`current_dependency_id = original_producer_id`

1. **Serialization Node:**
    * `serializer_node_id = {:serialize, original_producer_id, serialize_function_spec}`
    * User's serialization MFA: `ser_mfa = arg_spec.serialization_fn || {Handoff.InternalOps, :identity_with_nodes, []}`.
    * Create and add a new `Handoff.Function`:
        * `id`: `serializer_node_id`
        * `args`: `[current_dependency_id]`
        * `code`: store the MFA and let executor handle `apply`.
        * `extra_args`: `elem(ser_mfa, 2)` (static args from user's MFA).
        * `type`: `:regular` (as it might involve I/O or significant work).
        * `node`: `{:collocated, original_producer_id}`.
        * `cost`: User can potentially specify cost for ser/deser in `Function.Argument` later, for now `nil`.
    * `current_dependency_id = serializer_node_id`

2. **Deserialization Node:**
    * `deserializer_node_id = {:deserialize, original_producer_id, deserialize_function_spec}`
    * User's deserialization MFA: `deser_mfa = arg_spec.deserialization_fn || {Handoff.InternalOps, :identity_with_nodes, []}`.
    * Create and add a new `Handoff.Function`:
        * `id`: `deserializer_node_id`
        * `args`: `[current_dependency_id]` (output of serializer node).
        * `code`: Similar to serializer, stores the base MFA.
        * `extra_args`: `elem(deser_mfa, 2)`.
        * `type`: `:regular`.
        * `node`: `{:collocated, consuming_function_id}`
        * `cost`: `nil`.
    * `current_dependency_id = deserializer_node_id`

3. The entry for `arg_spec` in `F.args` is replaced with `deserializer_node_id`.

### 2.4. `Handoff.InternalOps` Module

This new module will house helper functions.

```elixir
defmodule Handoff.InternalOps do
  @moduledoc "Internal helper functions for Handoff operations."

  @doc """
  Identity function that expects node information but ignores it.
  Used as a default if user doesn't provide a ser/deser function.
  Signature: data, source_node, target_node, any_static_args...
  """
  def identity_with_nodes(data, _source_node, _target_node, _extra_args \\ []) do
    data
  end
end
```

### 2.5. Allocator (`Handoff.SimpleAllocator`) Modifications

No changes needed to the allocator.

### 2.6. Executor (`Handoff.DistributedExecutor` & `Handoff.RemoteExecutionWrapper`) Modifications

The executor is responsible for correctly invoking the `code` of these synthetic and user-defined functions, including injecting dynamic arguments.

* **Argument Injection for Serialization/Deserialization Functions:**
    When `execute_function_on_node` (or its equivalent in `RemoteExecutionWrapper`) is about to run a function that is a user-defined serializer or deserializer (identified by its ID pattern or a flag in the `Handoff.Function` struct if we add one):
    1. Let `current_function` be the function being executed (e.g., the serializer node or deserializer node). Let its allocated node be `current_function_node`.
    2. `data_input`: Result from `current_function.args[0]` (which is the ID of the dependency).
    3. Determine `effective_source_node` and `effective_target_node` for the user's function call:
        * If `current_function` is a **serializer node** (e.g., ID `{:serialize, original_producer_id, _}` where `original_producer_id` is the ID of the actual data source function, and the `_` could be the consuming function's ID or the `serialize_function_spec` for uniqueness):
            * `effective_source_node` = `current_function_node`. (This node is also where `original_producer_id` runs, due to the serializer's `node: {:collocated, original_producer_id}` directive).
            * To find `effective_target_node`:
                * The serializer's direct consumer in the DAG is the corresponding deserializer node (e.g., `{:deserialize, original_producer_id, _}`).
                * The deserializer node is defined with a `node: {:collocated, consuming_function_id}` directive, where `consuming_function_id` is the ID of the ultimate user function that consumes this argument.
                * Therefore, `effective_target_node` is the node allocated to this `consuming_function_id`. The executor must look up the allocated node of `consuming_function_id`.
        * If `current_function` is a **deserializer node** (e.g., ID `{:deserialize, original_producer_id, _}`):
            * To find `effective_source_node`:
                * The deserializer's direct producer in the DAG is the corresponding serializer node (e.g., `{:serialize, original_producer_id, _}`).
                * The serializer node is defined with `node: {:collocated, original_producer_id}`.
                * Therefore, `effective_source_node` is the node allocated to `original_producer_id`. The executor must look up the allocated node of `original_producer_id`.
            * `effective_target_node` = `current_function_node`. (This node is also where the `consuming_function_id` runs, due to the deserializer's `node: {:collocated, consuming_function_id}` directive).
    5. The `user_mfa = current_function.code` (assuming `code` stores the MFA tuple).
    6. Invoke: `apply(elem(user_mfa,0), elem(user_mfa,1), [data_input, effective_source_node, effective_target_node | current_function.extra_args])`.

* **Argument Injection for `Handoff.InternalOps.extract_element`:**
    1. `data_input`: Result from `current_function.args[0]`.
    2. Invoke: `apply(Handoff.InternalOps, :extract_element, [[data_input | current_function.extra_args]])`. (Note: `extract_element` expects a single list arg).

* **Result Forwarding:** The `ResultStore` will store the output of these transformation functions like any other. `DataLocationRegistry` will track their locations.

## 3. Signature of User-Provided Ser/Deser Functions

Users must define their serialization/deserialization functions with the following signature if they are to be used in `Handoff.Function.Argument`:

```elixir
def my_serialization_function(data_to_process, source_node_atom, target_node_atom, any_static_extra_args...) do
  # Example:
  # if source_node_atom == target_node_atom do
  #   # Maybe just pass data through, or a lighter transformation
  #   {:ok, data_to_process, :no_serialize}
  # else
  #   # Perform full serialization
  #   {:ok, :erlang.term_to_binary(data_to_process), :serialized}
  # end
  data_to_process # or transformed/serialized data
end

def my_deserialization_function(received_data, source_node_atom, target_node_atom, any_static_extra_args...) do
  # Example:
  # if source_node_atom == target_node_atom do
  #   # Data was not serialized by the pair function
  #   received_data
  # else
  #   # Perform full deserialization
  #   :erlang.binary_to_term(received_data)
  # end
  received_data # or transformed/deserialized data
end
```

The executor will prepend `data`, `source_node`, and `target_node` to the `static_extra_args` list when calling.

## 4. Implementation Steps Outline

1. **Define `Handoff.Function.Argument` struct.**
2. **Define `Handoff.InternalOps` module** with `extract_element/1` and `identity_with_nodes/3`.
3. **Modify `Handoff.Function` struct** to update the type of `args`.
4. **Modify `Handoff.DAG.add_function/2`:**
    * Implement logic to detect `Handoff.Function.Argument` in `args`.
    * Implement creation of synthetic nodes for extraction, serialization, and deserialization.
    * Generate unique IDs for synthetic nodes.
    * Correctly link dependencies between original, synthetic, and consuming functions.
5. **Modify `Handoff.SimpleAllocator`:**
    * Add support for `{:colocate_with_input, index}` directive. This requires resolving the assigned node of a dependency.
    * Add support for `:colocate_with_consumer` directive. This requires identifying the consuming function and its assigned node. This might involve iterating assignments or graph traversal.
6. **Modify `Handoff.DistributedExecutor`:**
    * Update `fetch_arguments` and related functions to correctly handle the new synthetic nodes.
    * Implement the argument injection logic in `execute_function_on_node` (and potentially `_execute_inline_local` if extract is inline and needs node info, though `extract_element` as defined doesn't).
    * Ensure `source_node` and `target_node` are correctly determined and passed.
        * `source_node` is where the current function is executing.
        * `target_node` for a serializer is the node of its consumer (the deserializer).
        * `source_node` for a deserializer is the node of its producer (the serializer). `target_node` for a deserializer is its own execution node.
7. **Modify `Handoff.RemoteExecutionWrapper` (if applicable):**
    * Ensure it aligns with `DistributedExecutor`'s changes if it directly invokes function code or manages argument preparation for remote calls that involve these synthetic functions.
8. **Add comprehensive tests:**
    * Unit tests for `Handoff.Function.Argument`.
    * Unit tests for `Handoff.InternalOps`.
    * Integration tests for `Handoff.DAG.add_function` DAG rewriting.
    * Integration tests for allocator with new colocation directives.
    * End-to-end tests for DAGs involving tuple extraction.
    * End-to-end tests for DAGs involving serialization/deserialization, covering:
        * Same-node producer/consumer.
        * Different-node producer/consumer.
        * User functions correctly receiving node information.
        * Default identity functions.

## 5. Open Questions/Refinements During Implementation

* **Error Handling:** How are errors from user-provided ser/deser functions or extraction handled and propagated? (Likely standard function failure).
* **Performance of Default Identity:** If no user ser/deser function is provided, can we optimize away the synthetic ser/deser nodes entirely post-allocation if producer/consumer are on the same node? (Phase 2 optimization). For now, `identity_with_nodes` will run.
* **Complexity of Allocator:** The colocation logic, especially `:colocate_with_consumer`, might require multiple passes or more sophisticated graph analysis in the allocator.
* **Clarity of Node Info for User Functions:** Ensure the `source_node` and `target_node` passed to user functions are clearly defined and consistently represent the nodes involved in the potential data transfer that the ser/deser pair is managing.

This plan provides a comprehensive approach to implementing the desired features. The most challenging aspects will be the DAG rewriting in `add_function` and the new colocation logic in the allocator.
