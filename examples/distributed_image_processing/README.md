# Distributed Image Processing Example

This example demonstrates using Handout for distributed image processing across multiple nodes.

## Prerequisites

- Elixir 1.18 or later
- Multiple connected Erlang nodes (can be on the same machine)
- [Image](https://hexdocs.pm/image/readme.html) library: `mix deps.get`

## Overview

The example:

1. Loads a set of images from a directory
2. Applies various transformations (resize, blur, grayscale, etc.)
3. Aggregates the results and creates a collage
4. Saves the output files

Each transformation is assigned resource requirements and distributed across available nodes.

## Running the Example

### 1. Start multiple nodes

```bash
# Terminal 1: Start the first node
iex --name node1@127.0.0.1 -S mix

# Terminal 2: Start the second node
iex --name node2@127.0.0.1 -S mix

# Terminal 3: Start the third node (optional)
iex --name node3@127.0.0.1 -S mix
```

### 2. Connect the nodes

In Terminal 1 (node1):

```elixir
Node.connect(:"node2@127.0.0.1")
Node.connect(:"node3@127.0.0.1")
```

### 3. Run the example

In Terminal 1 (node1):

```elixir
# Load the script
c "examples/distributed_image_processing/image_pipeline.exs"

# Run the pipeline
DistributedImageProcessing.run()
```

## Code Structure

- `image_pipeline.exs` - Main pipeline script
- `image_transformations.ex` - Image processing functions

## Key Concepts Demonstrated

- Distributed execution across nodes
- Resource-aware function allocation
- Complex DAG with multiple paths
- Dynamic node discovery
- Error handling and retries
- Resource monitoring during execution
