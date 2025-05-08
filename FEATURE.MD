# Feature: Inline Functions

## 1. Goal

Introduce a new function type, `:inline`, within a Directed Acyclic Graph (DAG).
Functions marked as `type: :inline`:

1. Have their results used as arguments by other "consumer" functions.
2. Are executed "just-in-time" on the same node where their consumer function is being executed.
3. Their results are directly passed to the consumer and are *not* stored in the global `ResultStore` nor registered in the `DataLocationRegistry`.
4. This replaces the previous `execute_for_side_effects_only` concept. If a function is purely for side-effects and needs this "inline" execution behavior, it's a special case of an inline function whose result is ignored by the consumer (though the system will compute it).

This meets the user's suggestion: "mark the functions with a type: :inline ... these functions should always be run on the node that's currently requesting their results." The "always be ran" is handled by JIT execution, and "instead of the value being saved" is handled by not using global stores.

## 2. Key Files for Modification

* `lib/handoff/dag.ex` (or wherever function structs/definitions reside)
* `lib/handoff/distributed_executor.ex`
* `lib/handoff/remote_execution_wrapper.ex`

## 3. Detailed Execution Plan

### Step 3.1: Update Function Definition

1. **Modify the function definition structure:**
    * Add a new field, `type: :regular | :inline`, to the structure/map that defines a function.
    * Default this field to `:regular`.
    * The `node` field (for pre-assigned execution node) is typically not applicable or ignored for `:inline` functions, as their execution node is determined by their consumer.
    * Example (conceptual):

        ```elixir
        # In the function definition map/struct
        %{
          id: :my_inline_function,
          code: {MyModule, :compute_something, []},
          args: [:another_arg_id], # Can have its own args
          extra_args: [],
          type: :inline # New field
          # cost: ... (Cost might still be relevant if consumer needs to account for it)
        }

        %{
          id: :my_consumer_function,
          code: {MyModule, :uses_something, []},
          args: [:my_inline_function, :regular_arg_id],
          extra_args: [],
          type: :regular
        }
        ```

### Step 3.2: Update DAG Validation (`Handoff.DAG.validate/1`)

1. **Validation Rules for Inline Functions:**
    * Existing rules (e.g., cycle detection) still apply. A cycle of purely inline functions would effectively mean a non-terminating recursion during argument resolution.
    * An `:inline` function should have its `node` attribute explicitly `nil`. The validator should enforce `node == nil` for `:inline` types.

### Step 3.3: Modify `Handoff.DistributedExecutor`

1. **Allocation (`allocate_functions/3`):**
    * Inline functions should be excluded from the allocation process that assigns functions to specific nodes, as their execution node is dynamic.
    * The `Handoff.SimpleAllocator.allocate` function might need to be made aware of these types or they should be filtered out before calling it.

2. **Argument Resolution for Local Consumers (`fetch_arguments/4`):**
    * This function is called by `execute_ready_function` when the *consumer* function is to be executed locally on the orchestrator.
    * Modify `fetch_arguments` (and its helper `resolve_argument/4`):
        * When an `arg_id` is encountered:
            1. Retrieve the definition of the function corresponding to `arg_id` from `dag.functions`.
            2. If `function_def.type == :inline`:
                * Invoke a new private helper, e.g., `_execute_inline_local(dag, function_def, executed_results_of_prior_regular_functions)`.
                * This helper will:
                    * Recursively call `fetch_arguments` for *this inline function's own arguments*.
                    * Execute the inline function: `Kernel.apply(function_def.code, resolved_args_for_inline ++ function_def.extra_args)`.
                    * Return the `result`. This result is the value for `arg_id`.
            3. If `function_def.type == :regular`:
                * Proceed with existing logic: check `executed_results`, then `ResultStore.get_with_fetch`.
    * The `executed_results` map passed around should only contain results of `:regular` functions.

3. **Execution Flow (`execute_ready_function/4`):**
    * After `fetch_arguments` (which now handles inline dependencies for local consumers), `args_for_execution` will contain actual values.
    * `execute_function_on_node` is then called for the *consumer* function.
        * If consumer is local, `execute_local` applies it with the resolved args.
        * If consumer is remote, `execute_remote` is called.

4. **Remote Execution Trigger (`execute_remote/3`):**
    * When making an RPC call to `Handoff.RemoteExecutionWrapper.execute_and_store` for a *consumer* function:
        * It must now also pass the complete `dag.functions` map (or at least all relevant function definitions). This allows the `RemoteExecutionWrapper` to identify and handle any `:inline` arguments of the consumer function on the remote node.
        * Signature becomes roughly: `:rpc.call(function.node, RemoteExecutionWrapper, :execute_and_store, [dag_id, consumer_function_struct, consumer_arg_ids, Node.self(), dag.functions])`.

5. **Result Handling:**
    * Results of `:inline` functions are transient and not stored via `ResultStore.store` or registered with `DataLocationRegistry`.
    * When a `:regular` function (that might have consumed inline functions) completes, its result is stored as usual.

### Step 3.4: Modify `Handoff.RemoteExecutionWrapper`

1. **Update `execute_and_store/5` (new arity):**
    * Receives `dag_id, consumer_function_struct, consumer_arg_ids, orchestrator_node, all_dag_functions`.

2. **Argument Resolution for Remote Consumers (`fetch_arguments/4` - new arity in Wrapper):**
    * This is the counterpart to `DistributedExecutor.fetch_arguments` but runs on the remote worker node.
    * Modify signature: `fetch_arguments(dag_id, arg_ids_for_consumer, orchestrator_node, all_dag_functions)`.
    * For each `arg_id` needed by the *consumer*:
        1. Retrieve `function_def = Map.get(all_dag_functions, arg_id)`.
        2. If `function_def.type == :inline`:
            * Invoke a new private helper, e.g., `_execute_inline_on_worker(dag_id, function_def, orchestrator_node, all_dag_functions)`.
            * This helper will:
                * Recursively call `fetch_arguments` (the wrapper's own version) for *this inline function's own arguments*.
                * Execute the inline function locally on this worker: `Kernel.apply(function_def.code, resolved_args_for_inline ++ function_def.extra_args)`.
                * Return the `result`.
        3. If `function_def.type == :regular`:
            * Proceed with existing logic: check local `ResultStore`, then orchestrator `DataLocationRegistry` lookup, then fetch from source node via RPC to its `ResultStore.get`. Cache locally if fetched.

3. **Execution:**
    * After its `fetch_arguments` resolves all arguments (including executing inlines locally on the worker), `execute_and_store` applies the *consumer* function.
    * The result of the *consumer* function is stored in the worker's local `ResultStore` (if it's a `:regular` function) and `{:ok, :result_stored_locally}` is returned as before. Inline functions themselves don't have their results stored here.

### Step 3.5: Testing Strategy

1. **DAG Validation:** Test that `node` field is handled correctly for inline functions.
2. **Local Execution:**
    * Consumer is local, argument is inline.
    * Consumer is local, argument is inline, which itself has an inline argument.
    * Consumer is local, argument is inline, which has a regular argument (that might be pre-computed or remote).
3. **Remote Execution:**
    * Consumer is remote, argument is inline (executed on remote worker).
    * Consumer is remote, argument is inline, which has an inline argument (chained execution on remote worker).
    * Consumer is remote, argument is inline, which has a regular argument (fetched by worker before inline execution).
4. **Mixed Scenarios:** DAGs with combinations of local/remote regular functions and inline functions.
5. **Error Propagation:** If an inline function fails during its JIT execution, the error should propagate to its consumer and halt further processing for that branch of the DAG.

## 4. Considerations

* **Performance:** Deeply nested inline functions or inline functions with many arguments requiring RPCs could lead to performance bottlenecks due to sequential JIT execution.
* **Complexity:** The argument resolution logic in both `DistributedExecutor` and `RemoteExecutionWrapper` becomes significantly more complex and recursive.
* **Side Effects of Inline Functions:** Side effects of an inline function will occur on the node where its consumer is executed, at the time of consumption. This is usually the desired behavior.
* **Cost Tracking:** If functions have `cost`, the cost of inline functions might need to be aggregated or considered by the consumer for resource management, though they are not "allocated" in the same way.
* **Clarity of `node` field:** The `node` field in a function definition for an `:inline` type is misleading. It should either be disallowed by validation or explicitly ignored.

This updated plan aims to integrate `type: :inline` functions as a core part of the execution model.
