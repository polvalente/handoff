# Execution Plan: Enabling Concurrent DAG Executions

## Preface for LLM

This document outlines the refactoring steps required to enable concurrent execution of multiple Directed Acyclic Graphs (DAGs) within the Handout system. The core idea is to introduce a `%DAG{}.id` that will be used to scope various operations, particularly data storage, retrieval, and location tracking. This will ensure that data belonging to one DAG execution does not interfere with another. The existing single-run functionality should be preserved and extended.

## I. DAG Identification and Propagation [IMPLEMENTED]

1. **DAG ID Generation:** [DONE]
    * A unique identifier (`id`) must be associated with each DAG execution. [DONE]
    * This ID can be generated automatically (i.e `make_ref()`) when a new DAG execution is initiated. [DONE]
    * Alternatively, the user could provide this ID, and then its up to the user to ensure uniqueness. [DONE]
2. **DAG ID Propagation:** [DONE - Core propagation implemented; DataLocationRegistry and other specific inter-node comms to be handled in Section II]
    * The `%DAG{}.id` must be passed down through the system, from the initial point of DAG submission to all components involved in its execution. [DONE]
    * This includes the `Executor`, `DistributedExecutor`, `ResultStore`, `DistributedResultStore`, `DataLocationRegistry`, and any inter-node communication mechanisms. [DONE for Executor, DistributedExecutor, ResultStore, DistributedResultStore; DataLocationRegistry pending in Sec II]
    * Consider adding `%DAG{}.id` as a parameter to relevant function calls or as part of the execution context. [DONE]

## II. Scoping Data Management by DAG ID [IMPLEMENTED]

1. **`DataLocationRegistry` Modifications:** [DONE]
    * The `DataLocationRegistry` must be updated to store and retrieve data locations (for arguments and results) scoped by `%DAG{}.id`. [DONE]
    * Option 1: Use a composite key like `{%DAG{}.id, data_id}` for lookups. [DONE]
    * Option 2: Maintain separate registries or namespaces per `%DAG{}.id`.
    * When data is registered, its `%DAG{}.id` must be recorded alongside its location. [DONE]
2. **`ResultStore` and `DistributedResultStore` Modifications:** [DONE]
    * All operations (put, get, delete and clear) in `ResultStore` and `DistributedResultStore` must be scoped by `%DAG{}.id`. [DONE]
    * This means that data written by one DAG cannot be accidentally read or overwritten by another. [DONE]
    * Storage keys should incorporate the `%DAG{}.id` (e.g., `%DAG{}.id:result_id` or a nested structure). [DONE]
    * The orchestrator, when notified of result metadata, must also store this metadata in a way that's queryable by `%DAG{}.id`. [DONE]
3. **Argument Fetching and Caching Modifications:** [DONE]
    * When a task on a worker node requires an argument:
        * **Local Cache Check:** The node's result store (cache) must be checked using both `%DAG{}.id` and `argument_id`. [DONE]
        * **Location Query:** If not in cache, query the `DataLocationRegistry` using `%DAG{}.id` and `argument_id`. [DONE]
        * **Direct Fetch:** Request the argument from the source node, including the `%DAG{}.id` in the request. [DONE]
        * **Cache Update:** Upon receiving the argument, store it in the local KV store scoped by `%DAG{}.id` and `argument_id`. [DONE]

## III. Executor and Inter-Node Communication Changes [IMPLEMENTED]

1. **`Executor` and `DistributedExecutor` Modifications:** [DONE]
    * The `Executor` and `DistributedExecutor` must be aware of the `%DAG{}.id` for the tasks they are processing. [DONE]
    * When dispatching tasks, scheduling work, or interacting with other components (like result stores or data registries), the `%DAG{}.id` must be used. [DONE]
    * Task execution contexts should be clearly associated with a `%DAG{}.id`. [DONE]
2. **Inter-Node Communication Protocol:** [DONE]
    * All inter-node communication related to data fetching (arguments or results) or metadata exchange must include the `%DAG{}.id`. [DONE]
    * This ensures that requests are routed to the correct data scope and that responses are correctly associated with the requesting DAG. [DONE]
    * Serialization/deserialization mechanisms should be reviewed to handle `%DAG{}.id` if necessary. [DONE - Standard types used, no change needed]

## IV. API Considerations [IMPLEMENTED]

1. **DAG Submission API:** [DONE]
    * The API for submitting a new DAG for execution might need to be updated to accept an optional `%DAG{}.id`. If not provided, the system should generate one. [DONE - Handled by Handout.DAG.new/0 and Handout.DAG.new/1]
    * The submission response should include the `%DAG{}.id` used for this execution. [DONE - Executor and DistributedExecutor now return %{dag_id: ..., results: ...}]
2. **Result Retrieval API:** [DONE]
    * APIs for retrieving results or checking the status of a DAG execution must allow specifying the `%DAG{}.id`. [DONE - Relevant functions in Handout module now take dag_id]
    * This ensures users can fetch results for a specific DAG run, especially when multiple instances of the same DAG definition might be running concurrently. [DONE]

## V. Testing Strategy

1. **Unit Tests:**
    * Test `DataLocationRegistry` with `%DAG{}.id` scoping (registration, lookup for same/different DAGs).
    * Test node-local argument KV store with `%DAG{}.id` scoping.
    * Test `ResultStore` and `DistributedResultStore` operations (put, get) ensuring data isolation between `%DAG{}.id`s.
2. **Integration Tests:**
    * **Concurrent DAG Execution:** Run two or more simple DAGs concurrently.
        * Verify that argument fetching for DAG A (requiring data from a remote node) is not affected by and does not affect DAG B.
        * Verify that results for DAG A are stored and retrieved correctly, isolated from DAG B's results.
        * Test scenarios where tasks from different DAGs run on the same worker node.
    * **End-to-End Scenarios:**
        * Submit multiple DAGs (can be instances of the same DAG definition or different DAGs) concurrently.
        * Verify that all DAGs complete successfully and their results are correct and independently retrievable using their respective `%DAG{}.id`s.
        * Test error handling when a `%DAG{}.id` is incorrect or data is not found for a given `%DAG{}.id`.
3. **Stress Tests (Optional, for future):**
    * Test system behavior with a large number of concurrent DAGs to identify potential bottlenecks or resource contention issues related to `%DAG{}.id` management.
