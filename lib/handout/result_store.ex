defmodule Handout.ResultStore do
  @moduledoc """
  Provides storage and retrieval of function execution results and cached arguments.

  Maintains an ETS table for fast access to results and cached arguments by ID.
  The store can also fetch data from remote nodes when needed using the DataLocationRegistry.
  """

  use GenServer
  require Logger

  # Client API

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Stores the result of a function execution or caches an argument.

  ## Parameters
  - id: The ID of the function result or argument
  - value: The value to store
  """
  def store(id, value) do
    GenServer.call(__MODULE__, {:store, id, value})
  end

  @doc """
  Retrieves a value by its ID from the local store.

  ## Parameters
  - id: The ID of the value to retrieve

  ## Returns
  - {:ok, value} if the value is found
  - {:error, :not_found} if no value exists for the ID
  """
  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  @doc """
  Retrieves a value, fetching it from a remote node if necessary.

  ## Parameters
  - id: The ID of the value to retrieve
  - from_node: Optional node to fetch from directly

  ## Returns
  - {:ok, value} if the value is found or successfully fetched
  - {:error, :not_found} if the value couldn't be found
  - {:error, reason} for other errors
  """
  def get_with_fetch(id, from_node \\ nil) do
    # First check locally
    case get(id) do
      {:ok, value} ->
        {:ok, value}

      {:error, :not_found} ->
        fetch_remote(id, from_node)
    end
  end

  @doc """
  Fetches a value from a remote node and stores it locally.

  ## Parameters
  - id: The ID of the value to fetch
  - from_node: Specific node to fetch from, or nil to look up in registry

  ## Returns
  - {:ok, value} if successfully fetched
  - {:error, reason} if fetch failed
  """
  def fetch_remote(id, from_node \\ nil) do
    # If not given a specific node, look up in registry
    source_node =
      if from_node do
        from_node
      else
        case Handout.DataLocationRegistry.lookup(id) do
          {:ok, node_id} -> node_id
          {:error, :not_found} -> nil
        end
      end

    if source_node && source_node != Node.self() do
      # Try to fetch from the source node
      case :rpc.call(source_node, __MODULE__, :get, [id]) do
        {:ok, value} ->
          # Cache the fetched value locally
          store(id, value)
          {:ok, value}

        {:error, reason} ->
          {:error, reason}

        {:badrpc, reason} ->
          Logger.error(
            "Failed to fetch value #{inspect(id)} from node #{inspect(source_node)}: #{inspect(reason)}"
          )

          {:error, reason}
      end
    else
      {:error, :not_found}
    end
  end

  @doc """
  Checks if a value exists for the given ID.

  ## Parameters
  - id: The ID to check

  ## Returns
  - true if a value exists
  - false otherwise
  """
  def has_value?(id) do
    GenServer.call(__MODULE__, {:has_value, id})
  end

  @doc """
  Clears all stored values.
  """
  def clear do
    GenServer.call(__MODULE__, :clear)
  end

  # Server callbacks

  @impl true
  def init(_) do
    Logger.info("ResultStore init: #{inspect({self(), Node.self()})}")
    table = :ets.new(:handout_results, [:set, :private, read_concurrency: true])
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:store, id, value}, _from, state) do
    :ets.insert(state.table, {id, value})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    result =
      case :ets.lookup(state.table, id) do
        [{^id, value}] -> {:ok, value}
        [] -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:has_value, id}, _from, state) do
    result = :ets.member(state.table, id)
    {:reply, result, state}
  end

  @impl true
  def handle_call(:clear, _from, state) do
    :ets.delete_all_objects(state.table)
    {:reply, :ok, state}
  end
end
