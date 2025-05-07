defmodule Handoff.DistributedResultStore do
  @moduledoc """
  Provides synchronized storage and retrieval of function execution results across cluster nodes.

  Extends the local ResultStore with capabilities to synchronize results between nodes.
  """

  use GenServer

  alias Handoff.DataLocationRegistry
  alias Handoff.ResultStore

  require Logger

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stores a function result locally on the node where it was produced for a specific DAG.
  Registers the result location in the DataLocationRegistry but does not broadcast it.

  ## Parameters
  - dag_id: The ID of the DAG
  - function_id: The ID of the function
  - result: The result to store
  - origin_node: The node where the result was produced
  """
  def store_distributed(dag_id, function_id, result, origin_node \\ Node.self()) do
    # Store locally if this is the origin node
    if origin_node == Node.self() do
      ResultStore.store(dag_id, function_id, result)
    end

    # Register the result location
    DataLocationRegistry.register(dag_id, function_id, origin_node)

    :ok
  end

  @doc """
  Explicitly broadcasts a result to all connected nodes for a specific DAG.

  Use this only when a result needs to be available on all nodes.

  ## Parameters
  - dag_id: The ID of the DAG
  - function_id: The ID of the function
  - result: The result to broadcast
  """
  def broadcast_result(dag_id, function_id, result) do
    # Store locally first
    ResultStore.store(dag_id, function_id, result)

    # Broadcast to other nodes
    GenServer.cast(__MODULE__, {:broadcast_result, dag_id, function_id, result})

    :ok
  end

  @doc """
  Retrieves a result for a specific DAG, potentially fetching it from its origin node.

  ## Parameters
  - dag_id: The ID of the DAG
  - function_id: The ID of the function
  - timeout: Maximum time to wait in milliseconds, defaults to 5000

  ## Returns
  - {:ok, result} on success
  - {:error, :timeout} if the result is not available within the timeout
  """
  def get_with_timeout(dag_id, function_id, timeout \\ 5000) do
    # Start a task to wait for the result
    task =
      Task.async(fn ->
        wait_for_result(dag_id, function_id, timeout)
      end)

    # Wait for the result or timeout
    Task.await(task, timeout + 500)
  end

  defp wait_for_result(dag_id, function_id, timeout) do
    start = System.monotonic_time(:millisecond)
    wait_loop(dag_id, function_id, start, timeout)
  end

  defp wait_loop(dag_id, function_id, start, timeout) do
    # Try to get value with automatic remote fetching
    case ResultStore.get_with_fetch(dag_id, function_id) do
      {:ok, result} ->
        # Result found or fetched
        {:ok, result}

      {:error, _reason} ->
        # Check if we've exceeded timeout
        now = System.monotonic_time(:millisecond)

        if now - start > timeout do
          {:error, :timeout}
        else
          # Wait a bit and try again
          :timer.sleep(100)
          wait_loop(dag_id, function_id, start, timeout)
        end
    end
  end

  @doc """
  Clears all results on all connected nodes for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG to clear
  """
  def clear_all_nodes(dag_id) do
    # Clear local results for the DAG
    ResultStore.clear(dag_id)
    # Clear local DataLocationRegistry for the DAG synchronously
    DataLocationRegistry.clear(dag_id)

    # Request all connected nodes to clear their results for the DAG (async)
    GenServer.cast(__MODULE__, {:broadcast_clear, dag_id})

    :ok
  end

  @doc """
  Synchronizes specific results from their origin nodes for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - function_ids: List of function IDs to synchronize

  ## Returns
  - Map of function_id => result for successfully synchronized results
  """
  def synchronize(dag_id, function_ids) do
    # Use get_with_fetch to find and sync results for the DAG
    Enum.reduce(function_ids, %{}, fn function_id, acc ->
      case ResultStore.get_with_fetch(dag_id, function_id) do
        {:ok, result} ->
          Map.put(acc, function_id, result)

        {:error, _} ->
          acc
      end
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
  def handle_cast({:broadcast_result, dag_id, function_id, result}, state) do
    # Send the result to all other nodes for the specific DAG
    Enum.each(Node.list(), fn node ->
      :rpc.cast(node, ResultStore, :store, [dag_id, function_id, result])
    end)

    {:noreply, state}
  end

  @impl true
  def handle_cast({:broadcast_clear, dag_id}, state) do
    # Send clear command to all other nodes for the specific DAG
    Enum.each(Node.list(), fn node ->
      :rpc.cast(node, ResultStore, :clear, [dag_id])
    end)

    # Also clear the data location registry for the specific DAG
    # (on this node, if it also received the cast)
    # This might be redundant if the caller node also called
    # clear_all_nodes, but ensures cleanup.
    DataLocationRegistry.clear(dag_id)

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodeup, node}, state) do
    Logger.info("Node #{inspect(node)} connected")
    # Don't automatically synchronize results anymore
    # Each node will fetch results as needed

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    Logger.warning("Node #{inspect(node)} disconnected")

    {:noreply, state}
  end
end
