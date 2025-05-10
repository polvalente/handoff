# Documentation Improvement Plan

This document outlines the steps to improve the `README.md` and livebook examples for the Handoff project.

## Phase 1: Analyze Existing Documentation and Code

- [X] Read and understand the current `README.md` to identify areas for improvement, outdated information, and missing sections. (Note: Key issue of `fn` usage identified and addressed in README examples and core moduledocs)
- [X] Review all livebooks in the `livebooks/` directory (`distributed_image_processing.livemd`, `nx_pipeline.livemd`, `simple_pipeline.livemd`, `README.md`):
  - [X] Assess their clarity, correctness, and completeness. (Note: `fn` usage identified as major issue to be fixed in Phase 3)
  - [X] Identify any discrepancies with the current library features. (Note: `fn` usage is main discrepancy, hardcoded paths in `nx_pipeline.livemd` also noted)
- [X] Examine the tests in `test/handoff/` to gain a deeper understanding of the library's functionalities, usage patterns, and edge cases. This will help ensure the documentation accurately reflects how the library is intended to be used. (Note: `dag_test.exs`, `distributed_executor_test.exs` confirmed API relies on captures/MFA, not `fn`)
- [X] Study the core modules in `lib/handoff/` (e.g., `dag.ex`, `distributed_executor.ex`) to confirm that the documentation and examples align with the actual implementation. (Note: `function.ex`, `dag.ex`, `distributed_executor.ex` reviewed; `dag.ex` validation explicitly disallows `fn` for `Handoff.Function` code. Moduledocs for `function.ex` and `dag.ex` updated.)

## Phase 2: Improve README.md

- [X] **Update "Usage" Section**:
  - [X] Revise the "Basic DAG Construction" example to be more comprehensive and reflect any API changes.
  - [X] Add a subsection for "Executing a DAG" with a clear example, based on the current implementation in `Handoff.DistributedExecutor`.
- [X] **Update "Distributed Execution" Section**:
  - [X] Provide a clear example and explanation for distributed execution using `Handoff.DistributedExecutor`.
  - [X] Explain how to achieve local-only execution using `Handoff.DistributedExecutor`.
- [ ] **Verify Installation Instructions**:
  - [ ] Confirm the `mix.exs` dependency snippet is correct and the version is current. (User confirms `{:handoff, "~> 0.1.0"}` is okay for release soon).
- [X] **Add "Running Tests" Section**:
  - [X] Provide instructions on how to run the test suite (e.g., `mix test`).
- [X] **Add "Building Documentation" Section**:
- [X] If ExDoc is used or planned, provide instructions on how to generate local documentation (e.g., `mix docs`).
- [X] **Review and Update "Features" Section**:
  - [X] Ensure all listed features are implemented and accurately described.
  - [X] Add any new significant features.
- [X] **Check Links**:
  - [X] Verify that all internal and external links (e.g., to `CONTRIBUTING.md`, `LICENSE`, HexDocs) are correct and functional. (Note: License link updated to `LICENSE` and text to Apache License by user).
- [X] **General Readability and Structure**:
  - [X] Improve overall clarity, conciseness, and organization.
  - [X] Ensure consistent formatting.

## Phase 3: Improve Livebooks

- [ ] **General for all Livebooks**:
  - [ ] Ensure all code examples run without errors using the latest version of the library.
  - [ ] Update code and explanations to reflect any API changes or new features.
  - [ ] Add more detailed explanations, comments, and narrative to guide the user.
  - [ ] Ensure each livebook has a clear objective and learning outcomes.
- [ ] **Review `livebooks/simple_pipeline.livemd`**:
  - [ ] Verify it serves as a good entry point for understanding core Handoff concepts.
  - [ ] Expand on explanations of `Handoff.DAG`, `Handoff.Function`, and basic execution.
- [ ] **Review `livebooks/distributed_image_processing.livemd`**:
  - [ ] This is a key example for showcasing advanced features. Ensure it accurately demonstrates distributed execution and resource-aware scheduling (if available and functional).
  - [ ] Simplify the example if it's too complex for an initial demonstration, or break it into smaller, digestible parts.
  - [ ] Clearly explain the setup required for distributed execution.
- [ ] **Review `livebooks/nx_pipeline.livemd`**:
  - [ ] Ensure the integration with Nx is clear and the example is relevant.
  - [ ] Highlight the benefits of using Handoff with Nx.
- [ ] **Review `livebooks/README.md`**:
  - [ ] Update the README in the `livebooks/` directory to provide an overview of the available livebooks and how to run them.
  - [ ] Ensure it mentions any specific dependencies or setup needed for the livebooks (e.g., installing IEx if not obvious).
- [ ] **Consider New Livebook Examples**:
  - [ ] Identify if there are other common use cases or important features not covered by the current livebooks (e.g., fault tolerance, specific scheduler configurations).

## Phase 4: General Documentation Review and Finalization

- [ ] **Consistency Check**:
  - [ ] Ensure consistent terminology, code style, and formatting across `README.md` and all livebooks.
- [ ] **Proofreading**:
  - [ ] Thoroughly review all content for typos, grammatical errors, and awkward phrasing.
- [ ] **Clarity and Audience**:
  - [ ] Ensure the documentation is clear, concise, and targeted appropriately for Elixir developers who may be new to Handoff.
- [ ] **Update `DOCUMENTATION.md`**:
  - [ ] Mark completed tasks in this plan.
  - [ ] Add any new tasks or notes that arise during the process.
