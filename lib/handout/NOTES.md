- Functions add themselves as dependencies for their args in Handout.DAG
- Currently no way of depending on just a single position of an argument
  - We can fix this by introducing a special syntax where arguments can be a tuple of {:fetch, id, index}
    and we fetch the index with elem or Enum.fetch depending on whether the value is a tuple.
    The fetch must happen in the source node before sending data to the dependent node.
- Distributed executor is always accumulating results on the orchestrator instead of keeping them on-node. And arguments are fetched from the locally saved map. We need to change this so that the arguments are fecthed from the node in which they are allocated and results are saved locally to each node only, and we just transfer when requested. Each node can keep a cache of the fetched arguments so that if multiple functions fetch the same arguments to the same node, the data only travels once.
- Change graph validations in DAG to use :digraph and :digraph_utils instead of custom functions
- Distributed result store shouldn't sync results by default.
- Possibly remove Executor in favor of just using DistributedExecutor for both cases. Likewise, Handout.ResultStore can also be the same as the DistributedResultStore
- Function: add possibility of forcing a function to have the same node as another with `node: `{:collocated, :another_function_id}`