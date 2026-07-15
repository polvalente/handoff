defmodule Handoff.DistributedExecutor do
  @moduledoc """
  Handles the distributed execution of functions across multiple nodes.

  Provides node discovery, coordination, and admission of per-DAG runners.
  Owns exactly-once replies to `execute/2` callers.
  """

  use GenServer

  alias Handoff.DAG
  alias Handoff.DataLocationRegistry
  alias Handoff.ExecutionSupervisor
  alias Handoff.ResultStore
  alias Handoff.SimpleResourceTracker

  require Logger

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Discovers and registers nodes in the cluster with their capabilities.

  Makes remote calls to discover nodes and their resources.
  """
  def discover_nodes do
    GenServer.call(__MODULE__, :discover_nodes)
  end

  @doc """
  Executes the DAG across the distributed nodes.

  ## Parameters
  - dag: A validated DAG to execute
  - opts: Optional execution options
    - `:max_retries` - default retries per function (default: 3);
      overridden by `Handoff.Function.max_retries` when set
    - `:max_concurrency` - max parallel ready functions (default: schedulers_online)

  ## Returns
  - `{:ok, results}` with a map of function IDs to results on success
  - `{:error, reason}` on failure
  """
  def execute(dag, opts \\ []) do
    case DAG.validate(dag) do
      :ok -> GenServer.call(__MODULE__, {:execute, dag, opts}, :infinity)
      error -> error
    end
  end

  # Server callbacks

  @impl true
  def init(opts) do
    heartbeat_interval = Keyword.get(opts, :heartbeat_interval, 5000)
    resource_tracker = Keyword.get(opts, :resource_tracker, SimpleResourceTracker)

    :timer.send_interval(heartbeat_interval, :check_nodes)

    {:ok,
     %{
       nodes: %{},
       # runner_pid => %{from: from, monitor: ref}
       executing: %{},
       max_retries: Keyword.get(opts, :max_retries, 3),
       resource_tracker: resource_tracker
     }}
  end

  @impl true
  def handle_call(:discover_nodes, _from, state) do
    {discovered, state} = do_discover_nodes(state)
    {:reply, {:ok, discovered}, state}
  end

  def handle_call({:execute, dag, opts}, from, state) do
    ResultStore.clear(dag.id)
    DataLocationRegistry.clear(dag.id)
    register_literal_args(dag)

    state =
      if map_size(state.nodes) == 0 do
        {_discovered, state} = do_discover_nodes(state)
        state
      else
        state
      end

    max_retries = Keyword.get(opts, :max_retries, state.max_retries)
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())
    {caller_pid, _tag} = from

    runner_opts = [
      dag: dag,
      executor: self(),
      resource_tracker: state.resource_tracker,
      nodes: cluster_nodes(),
      caller_pid: caller_pid,
      max_retries: max_retries,
      max_concurrency: max_concurrency
    ]

    case ExecutionSupervisor.start_runner(runner_opts) do
      {:ok, runner_pid} ->
        monitor = Process.monitor(runner_pid)

        executing =
          Map.put(state.executing, runner_pid, %{from: from, monitor: monitor})

        {:noreply, %{state | executing: executing}}

      {:error, reason} ->
        {:reply, {:error, {:runner_start_failed, reason}}, state}
    end
  end

  def handle_call(:get_resource_tracker, _from, state) do
    {:reply, state.resource_tracker, state}
  end

  @impl true
  def handle_info({:dag_finished, runner_pid, outcome}, state) do
    case Map.pop(state.executing, runner_pid) do
      {nil, _executing} ->
        # Already handled via :DOWN or duplicate finish
        {:noreply, state}

      {%{from: from, monitor: monitor}, executing} ->
        Process.demonitor(monitor, [:flush])
        GenServer.reply(from, outcome)
        {:noreply, %{state | executing: executing}}
    end
  end

  def handle_info({:DOWN, monitor, :process, runner_pid, reason}, state) do
    case Map.get(state.executing, runner_pid) do
      nil ->
        {:noreply, state}

      %{from: from, monitor: ^monitor} when reason != :normal ->
        # Crash before (or without) a finish message
        Logger.warning("DAG runner crashed: #{inspect(reason)}")
        {_entry, executing} = Map.pop(state.executing, runner_pid)
        GenServer.reply(from, {:error, {:dag_crashed, reason}})
        {:noreply, %{state | executing: executing}}

      %{monitor: ^monitor} ->
        # Normal exit — `:dag_finished` owns the reply (may already be queued)
        {:noreply, state}

      %{from: from} ->
        {_entry, executing} = Map.pop(state.executing, runner_pid)
        GenServer.reply(from, {:error, {:dag_crashed, reason}})
        {:noreply, %{state | executing: executing}}
    end
  end

  def handle_info(:check_nodes, state) do
    node_status =
      Enum.map(state.nodes, fn {node, _caps} ->
        is_alive = node == Node.self() or Node.ping(node) == :pong
        {node, is_alive}
      end)

    alive_nodes =
      Enum.reduce(node_status, state.nodes, fn
        {_node, true}, acc ->
          acc

        {node, false}, acc ->
          Logger.warning("Node #{inspect(node)} is down, removing from available nodes")
          Map.delete(acc, node)
      end)

    {:noreply, %{state | nodes: alive_nodes}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp do_discover_nodes(state) do
    tracker = state.resource_tracker

    discovered =
      Enum.reduce(cluster_nodes(), %{}, fn node, acc ->
        case :rpc.call(node, tracker, :get_capabilities, []) do
          {:badrpc, reason} ->
            Logger.warning(
              "Failed to discover capabilities for node #{inspect(node)}: #{inspect(reason)}"
            )

            acc

          capabilities when is_map(capabilities) ->
            Logger.info(
              "Discovered node #{inspect(node)} with capabilities: #{inspect(capabilities)}"
            )

            :ok = tracker.register(node, capabilities)
            Map.put(acc, node, capabilities)
        end
      end)

    {discovered, %{state | nodes: Map.merge(state.nodes, discovered)}}
  end

  defp register_literal_args(dag) do
    dag.functions
    |> Map.values()
    |> get_in([Access.all(), Access.key(:args)])
    |> List.flatten()
    |> Enum.each(fn
      nil ->
        :ok

      {:serialize, _, _, _, _} ->
        :ok

      {:deserialize, _, _, _, _} ->
        :ok

      arg_id ->
        if not Map.has_key?(dag.functions, arg_id) do
          DataLocationRegistry.register(dag.id, arg_id, Node.self())
        end
    end)
  end

  # Include hidden connections (e.g. peers attached to a Livebook `-hidden` runtime).
  defp cluster_nodes do
    Enum.uniq([Node.self() | Node.list(:connected)])
  end
end
