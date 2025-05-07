# Handoff Livebook Examples

This directory contains interactive Livebook examples for the Handoff library. These examples demonstrate how to build and execute directed acyclic graphs (DAGs) of operations in various scenarios.

## Available Examples

1. **Simple Pipeline** (`simple_pipeline.livemd`) - A basic example showing how to create a data processing pipeline with Handoff.
2. **Distributed Image Processing** (`distributed_image_processing.livemd`) - A more complex example demonstrating distributed processing across multiple nodes with resource requirements.

## Running the Examples

To run these examples, you need [Livebook](https://livebook.dev/) installed. You can install it with:

```bash
mix escript.install hex livebook
```

Or use the Docker image:

```bash
docker run -p 8080:8080 -p 8081:8081 livebook/livebook
```

Once Livebook is running, you can import and run these `.livemd` files.

## Key Features Demonstrated

These examples showcase several key features of the Handoff library:

- Building DAGs with dependencies between operations
- Executing DAGs in single-node and distributed environments
- Defining and managing resource requirements
- Error handling and retries
- Result passing between operations
- Parameterization of operations
