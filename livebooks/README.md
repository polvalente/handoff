# Handoff Livebook Examples

This directory contains interactive Livebook examples for the Handoff library. These examples demonstrate how to build and execute directed acyclic graphs (DAGs) of Handoff operations in various scenarios.

## Available Examples

1. **Simple Pipeline** (`simple_pipeline.livemd`)
    * **Focus**: Introduces core Handoff concepts like basic DAG construction, defining `Handoff.Function`s with captures, local execution using `Handoff.DistributedExecutor`, and parameterization.
    * **Dependencies**: None beyond Handoff itself.

2. **Distributed Image Processing** (`distributed_image_processing.livemd`)
    * **Focus**: Showcases more advanced features including distributed execution across multiple (simulated or real) Erlang nodes, resource-aware scheduling (`cost` attribute), and handling a more complex DAG with parallel branches.
    * **Dependencies**: Requires `{:image, "~> 0.14"}` (included in its `Mix.install`).
    * **Setup Notes**: This Livebook creates a `temp_images/` directory for placeholder input files. For image saving operations to succeed, you may need to manually create an `output/` directory in the root of the Handoff project workspace before running these parts of the Livebook. Instructions for setting up a multi-node environment for true distributed execution are included within the Livebook.

*(Note: `nx_pipeline.livemd` is currently under development and will be added here once ready.)*

## Running the Examples

To run these examples, you need [Livebook](https://livebook.dev/) (v0.12 or later recommended) installed. You can install it by following the instructions on the official Livebook website.

## Key Concepts Demonstrated Across Examples

These examples collectively showcase several key features of the Handoff library:

* Building DAGs with dependencies between operations using `Handoff.Function` and `Handoff.DAG`.
* Defining function logic using Elixir module function captures (`&Module.function/arity`).
* Executing DAGs in single-node and distributed environments using `Handoff.DistributedExecutor`.
* Defining and managing resource requirements (`:cost`) for functions.
* Parameterization of operations using `:extra_args`.
* Basic error handling and result processing.
