# Simple Data Pipeline Example

This example demonstrates using Handout to build a simple data processing pipeline.

## Overview

The pipeline performs the following steps:

1. Generate random data
2. Filter the data
3. Transform the data
4. Aggregate the results
5. Format the output

## Running the Example

```bash
# From the project root
cd examples/simple_pipeline
mix run pipeline.exs
```

## Code Structure

The example defines a DAG with five functions:

1. `generate_data` - Creates an array of random numbers
2. `filter_data` - Removes values below a threshold
3. `transform_data` - Applies a mathematical operation to each value
4. `aggregate_data` - Computes statistics on the transformed data
5. `format_output` - Formats the results for display

## Key Concepts Demonstrated

- Basic DAG construction
- Function dependencies
- Result passing between functions
- Error handling
- Parameterization via extra_args

## Extension Ideas

- Try running the pipeline with distributed execution
- Add resource requirements to functions
- Implement more complex transformations
- Add visualization of the execution flow
