defmodule Handout.DistributedResultStore do
  @moduledoc """
  Provides synchronized storage and retrieval of function execution results across cluster nodes.

  Extends the local ResultStore with capabilities to synchronize results between nodes.
  """

  use GenServer
  require Logger
  alias Handout.ResultStore

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stores a function result and broadcasts it to all connected nodes.

  ## Parameters
  - function_id: The ID of the function
  - result: The result to store
  - origin_node: The node where the result was produced
  """
  def store_distributed(function_id, result, origin_node \\ Node.self()) do
    # Store locally first
    ResultStore.store(function_id, result)

    # Broadcast to other nodes if this is the origin node
    if origin_node == Node.self() do
      GenServer.cast(__MODULE__, {:broadcast_result, function_id, result})
    end

    :ok
  end

  @doc """
  Retrieves a result, potentially waiting for it to be available.

  ## Parameters
  - function_id: The ID of the function
  - timeout: Maximum time to wait in milliseconds, defaults to 5000

  ## Returns
  - {:ok, result} on success
  - {:error, :timeout} if the result is not available within the timeout
  """
  def get_with_timeout(function_id, timeout \\ 5000) do
    # Start a task to wait for the result
    task =
      Task.async(fn ->
        start = System.monotonic_time(:millisecond)

        Stream.unfold(start, fn acc ->
          case ResultStore.get(function_id) do
            {:ok, result} ->
              {:halt, {:ok, result}}

            {:error, :not_found} ->
              now = System.monotonic_time(:millisecond)

              if now - start > timeout do
                {:halt, {:error, :timeout}}
              else
                # Wait a bit before trying again
                :timer.sleep(100)
                {nil, acc}
              end
          end
        end)
        |> Stream.run()
      end)

    # Wait for the result or timeout
    case Task.await(task, timeout + 500) do
      {:ok, result} -> {:ok, result}
      other -> other
    end
  end

  @doc """
  Clears all results on all connected nodes.
  """
  def clear_all_nodes do
    # Clear local results
    ResultStore.clear()

    # Request all connected nodes to clear their results
    GenServer.cast(__MODULE__, :broadcast_clear)

    :ok
  end

  @doc """
  Synchronizes specific results from their origin nodes.

  ## Parameters
  - function_ids: List of function IDs to synchronize
  - target_nodes: List of nodes to query, defaults to all connected nodes

  ## Returns
  - Map of function_id => result for successfully synchronized results
  """
  def synchronize(function_ids, target_nodes \\ nil) do
    nodes = target_nodes || [Node.self() | Node.list()]

    # Query each node for each function result
    Enum.reduce(function_ids, %{}, fn function_id, acc ->
      Enum.reduce_while(nodes, acc, fn node, inner_acc ->
        case :rpc.call(node, Handout.ResultStore, :get, [function_id]) do
          {:ok, result} ->
            # Store result locally and skip remaining nodes
            ResultStore.store(function_id, result)
            {:halt, Map.put(inner_acc, function_id, result)}

          {:error, :not_found} ->
            # Try next node
            {:cont, inner_acc}

          {:badrpc, reason} ->
            Logger.warning(
              "Error synchronizing result for function #{function_id} from node #{inspect(node)}: #{inspect(reason)}"
            )

            {:cont, inner_acc}
        end
      end)
    end)
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    # Subscribe to node up/down events
    :net_kernel.monitor_nodes(true)

    {:ok, %{}}
  end

  @impl true
  def handle_cast({:broadcast_result, function_id, result}, state) do
    # Send the result to all other nodes
    Node.list()
    |> Enum.each(fn node ->
      :rpc.cast(node, Handout.ResultStore, :store, [function_id, result])
    end)

    {:noreply, state}
  end

  @impl true
  def handle_cast(:broadcast_clear, state) do
    # Send clear command to all other nodes
    Node.list()
    |> Enum.each(fn node ->
      :rpc.cast(node, Handout.ResultStore, :clear, [])
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodeup, node}, state) do
    Logger.info("Node #{inspect(node)} connected, synchronizing results")

    # Get all local results
    all_results = get_all_local_results()

    # Send all results to the new node
    Enum.each(all_results, fn {function_id, result} ->
      :rpc.cast(node, Handout.ResultStore, :store, [function_id, result])
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    Logger.warning("Node #{inspect(node)} disconnected")

    {:noreply, state}
  end

  # Private helpers

  defp get_all_local_results do
    # This is a simple implementation that assumes the ETS table structure
    # In a real implementation, you might want a proper API in ResultStore
    try do
      :ets.tab2list(:handout_results)
    rescue
      ArgumentError -> []
    end
  end
end
