- Function: Currently no way of depending on just a single position of an argument
  - We can fix this by introducing a special syntax where arguments can be a tuple of {:fetch, id, index}
    and we fetch the index with elem or Enum.fetch depending on whether the value is a tuple.
    The fetch must happen in the source node before sending data to the dependent node.
- Function: add possibility of forcing a function to have the same node as another with `node: {:collocated, :another_function_id}`
- Function: some kind of annotation that a given function should always be ran instead of the value being saved. This will allow us to load certain layers of a neural network from disk regardless of where they're stored.
