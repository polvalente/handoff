# Execution Plan: Distributed Executor and Result Store Refactoring

## Preface for LLM

This document outlines the steps to refactor a distributed execution system. The primary goals are to:

1. **Reduce unnecessary data movement:** Arguments should be fetched directly from the node where they reside, and results should be stored locally on the computation node by default.
2. **Implement on-demand data transfer:** Data (arguments or results) should only be transferred across the network when explicitly requested by a task or the orchestrator.
3. **Introduce local caching for arguments:** Worker nodes should cache arguments fetched from other nodes to avoid redundant transfers if the same argument is needed multiple times by tasks on that node.

The system currently accumulates results on a central orchestrator and fetches arguments from a map that is assumed to be locally available or replicated. This plan details the changes to move towards a more efficient, decentralized data management strategy.

## I. Argument Management Overhaul

- [ ] **1. Data Location Registry:**
  - [ ] Design and implement a mechanism to track the location (node ID) of every piece of data (argument or intermediate result).
  - [ ] This could be a centralized registry on the orchestrator or a distributed data structure.
  - [ ] When data is created (e.g., initial arguments, function results), its ID and hosting node ID are registered.
- [ ] **2. Modify Argument Fetching in Task Execution:**
  - [ ] When a task on a worker node requires an argument:
    - [ ] **Local Cache Check:** First, check if the argument exists in the node's local argument Key-Value store.
    - [ ] **Location Query:** If not in cache, query the Data Location Registry to find the node holding the argument.
    - [ ] **Direct Fetch:** Request the argument directly from the source node.
    - [ ] **Cache Update:** Upon receiving the argument, store it in the local KV store.
- [ ] **3. Implement Node-Local Argument KV Store (Cache):**
  - [ ] Each worker node will maintain a simple Key-Value store for arguments fetched from other nodes. This store will not have an eviction policy for now.

## II. Result Management Overhaul

- [ ] **1. Local Result Storage:**
  - [ ] Modify the executor so that when a function completes on a worker node, its result is stored locally on that node.
  - [ ] The result's ID and its location (the current worker node's ID) are registered in the Data Location Registry.
  - [ ] The orchestrator is notified of the result's *metadata* (ID, location, type, etc.) but not the actual data.
- [ ] **2. On-Demand Result Transfer:**
  - [ ] Implement a mechanism for the orchestrator or other tasks/nodes to explicitly request a result.
  - [ ] When a result is requested (e.g., via a `get()` call on a future/result object):
    - [ ] The Data Location Registry is queried to find the node holding the result.
    - [ ] The result is fetched directly from that node.
- [ ] **3. Distributed Result Store Modification:**
  - [ ] Identify the existing `DistributedResultStore`.
  - [ ] Change its default behavior to *not* automatically synchronize or transfer result data to a central location (e.g., the orchestrator) upon computation.
  - [ ] Ensure that data is only moved when explicitly fetched.

## III. System-Wide Changes & Considerations

- [ ] **1. Inter-Node Communication Protocol:**
  - [ ] Define or refine the protocol for nodes to request and transfer data (arguments/results) directly between each other. This should handle serialization/deserialization.
- [ ] **2. Refactor `DistributedExecutor`:**
  - [ ] Update the `DistributedExecutor`'s logic for task scheduling, argument passing, and result handling to align with the new local storage and on-demand fetching model.
- [ ] **3. Error Handling & Fault Tolerance:**
  - [ ] Consider how to handle cases where a node holding requested data is unavailable.
  - [ ] Implement retries or error propagation mechanisms.
- [ ] **4. API Changes (if any):**
  - [ ] Determine if any existing user-facing APIs for submitting tasks or retrieving results need to change. Aim for minimal disruption if possible.

## IV. Testing Strategy

- [ ] **1. Unit Tests:**
  - [ ] Test the Data Location Registry (registration, lookup).
  - [ ] Test the node-local argument KV store (add, get).
  - [ ] Test data serialization/deserialization for inter-node communication.
- [ ] **2. Integration Tests:**
  - [ ] Test argument fetching: task on node A needing an argument from node B.
  - [ ] Test result storage: task on node A stores result locally, orchestrator knows location.
  - [ ] Test result fetching: orchestrator or task on node B fetches result from node A.
  - [ ] Test end-to-end scenarios with multiple tasks, dependencies, and nodes.
- [ ] **3. Performance Tests:**
  - [ ] Measure data transfer volumes before and after changes to quantify reduction.
  - [ ] Benchmark task execution times, especially for tasks with remote dependencies.
  - [ ] Test KV store (cache) effectiveness.
- [ ] **4. Correctness Tests for Result Store:**
  - [ ] Verify that results are not synced by default.
  - [ ] Verify that explicit fetch operations retrieve the correct data.
