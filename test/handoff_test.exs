defmodule HandoffTest do
  use ExUnit.Case, async: false

  alias Handoff.DAG
  alias Handoff.DistributedTestFunctions, as: F
  alias Handoff.Function
  alias Handoff.SimpleResourceTracker

  setup do
    SimpleResourceTracker.register(Node.self(), %{cpu: 4, memory: 2000})
    :ok
  end

  describe "execute_local/2" do
    test "runs the DAG and reports every function on the local node" do
      dag =
        DAG.new()
        |> DAG.add_function(%Function{
          id: :source,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [21]
        })
        |> DAG.add_function(%Function{id: :pair, args: [:source], code: &F.g/2, extra_args: [2]})

      assert {:ok, %{results: results, allocations: allocations}} = Handoff.execute_local(dag)
      assert results == %{source: 21, pair: [21, 2]}
      assert allocations == %{source: Node.self(), pair: Node.self()}
    end

    test "ignores node pinning and resource costs" do
      [node_2 | _] = Application.get_env(:handoff, :test_nodes)

      # The cost is more than any registered node can satisfy, so a regular
      # execute/2 would fail to allocate this function.
      dag =
        DAG.add_function(DAG.new(), %Function{
          id: :source,
          args: [],
          code: &Elixir.Function.identity/1,
          extra_args: [21],
          node: node_2,
          cost: %{cpu: 1000, memory: 1_000_000}
        })

      assert {:ok, %{results: %{source: 21}, allocations: %{source: local}}} =
               Handoff.execute_local(dag)

      assert local == Node.self()
    end
  end
end
