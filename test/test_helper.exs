case :os.type() do
  {:unix, _} ->
    {"", 0} = System.cmd("epmd", ["-daemon"])

  _ ->
    :ok
end

{:ok, _pid} = Node.start(:"primary@127.0.0.1")

# Start secondary and tertiary nodes
{:ok, _pid, node2} = :peer.start(%{name: :"secondary@127.0.0.1"})
{:ok, _pid, node3} = :peer.start(%{name: :"tertiary@127.0.0.1", args: ~w(-hidden)c})

# Set up code paths and ensure applications are started
for node <- [node2, node3] do
  true = :erpc.call(node, :code, :set_path, [:code.get_path()])

  :erpc.call(node, Application, :put_env, [
    :logger,
    :level,
    Application.get_env(:logger, :remote_node_level)
  ])

  {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:handoff])
end

# Store node names in application environment for tests to use
Application.put_env(:handoff, :test_nodes, [node2, node3])

ExUnit.start()
