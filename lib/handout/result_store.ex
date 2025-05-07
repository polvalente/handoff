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
  Stores the result of a function execution or caches an argument for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the function result or argument
  - value: The value to store
  """
  def store(dag_id, id, value) do
    GenServer.call(__MODULE__, {:store, dag_id, id, value})
  end

  @doc """
  Retrieves a value by its ID from the local store for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the value to retrieve

  ## Returns
  - {:ok, value} if the value is found
  - {:error, :not_found} if no value exists for the ID in the given DAG
  """
  def get(dag_id, id) do
    GenServer.call(__MODULE__, {:get, dag_id, id})
  end

  @doc """
  Retrieves a value, fetching it from a remote node if necessary for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the value to retrieve
  - from_node: Optional node to fetch from directly

  ## Returns
  - {:ok, value} if the value is found or successfully fetched
  - {:error, :not_found} if the value couldn't be found
  - {:error, reason} for other errors
  """
  def get_with_fetch(dag_id, id, from_node \\ nil) do
    # First check locally
    case get(dag_id, id) do
      {:ok, value} ->
        {:ok, value}

      {:error, :not_found} ->
        fetch_remote(dag_id, id, from_node)
    end
  end

  @doc """
  Fetches a value from a remote node and stores it locally for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the value to fetch
  - from_node: Specific node to fetch from, or nil to look up in registry

  ## Returns
  - {:ok, value} if successfully fetched
  - {:error, reason} if fetch failed
  """
  def fetch_remote(dag_id, id, from_node \\ nil) do
    # If not given a specific node, look up in registry
    # DataLocationRegistry lookup needs to be updated for dag_id as well
    source_node =
      if from_node do
        from_node
      else
        case Handout.DataLocationRegistry.lookup(dag_id, id) do
          {:ok, node_id} -> node_id
          {:error, :not_found} -> nil
        end
      end

    if source_node && source_node != Node.self() do
      # Try to fetch from the source node
      # The remote :get call must also pass dag_id
      case :rpc.call(source_node, __MODULE__, :get, [dag_id, id]) do
        {:ok, value} ->
          # Cache the fetched value locally
          store(dag_id, id, value)
          {:ok, value}

        {:error, reason} ->
          {:error, reason}

        {:badrpc, reason} ->
          Logger.error(
            "Failed to fetch value #{inspect(id)} for DAG #{inspect(dag_id)} from node #{inspect(source_node)}: #{inspect(reason)}"
          )

          {:error, reason}
      end
    else
      {:error, :not_found}
    end
  end

  @doc """
  Checks if a value exists for the given ID in a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID to check

  ## Returns
  - true if a value exists
  - false otherwise
  """
  def has_value?(dag_id, id) do
    GenServer.call(__MODULE__, {:has_value, dag_id, id})
  end

  @doc """
  Clears all stored values for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG whose results to clear
  """
  def clear(dag_id) do
    GenServer.call(__MODULE__, {:clear, dag_id})
  end

  # Server callbacks

  @impl true
  def init(_) do
    Logger.info("ResultStore init: #{inspect({self(), Node.self()})}")
    table = :ets.new(:handout_results, [:set, :private, :named_table, read_concurrency: true])
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:store, dag_id, id, value}, _from, state) do
    :ets.insert(state.table, {{dag_id, id}, value})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:get, dag_id, id}, _from, state) do
    require Logger

    Logger.info("ResultStore get: #{inspect(:ets.tab2list(state.table))}")

    result =
      case :ets.lookup(state.table, {dag_id, id}) do
        [{{^dag_id, ^id}, value}] -> {:ok, value}
        [] -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:has_value, dag_id, id}, _from, state) do
    result = :ets.member(state.table, {dag_id, id})
    {:reply, result, state}
  end

  @impl true
  def handle_call({:clear, dag_id}, _from, state) do
    match_spec = [{{{dag_id, :_}, :_}, [], [true]}]
    :ets.select_delete(state.table, match_spec)
    {:reply, :ok, state}
  end
end
