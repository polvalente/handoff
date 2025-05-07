defmodule Handout.DataLocationRegistry do
  @moduledoc """
  Tracks the location (node ID) of every piece of data (argument or intermediate result).

  This registry maintains a mapping of data IDs to their hosting nodes, enabling
  on-demand data fetching from the appropriate node.
  """

  use GenServer
  require Logger

  # Client API

  @doc """
  Starts the Data Location Registry GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Registers a data item with its hosting node.

  ## Parameters
  - data_id: The ID of the data (argument or result)
  - node_id: The node where the data is stored
  """
  def register(data_id, node_id) do
    GenServer.call(__MODULE__, {:register, data_id, node_id})
  end

  @doc """
  Looks up where a data item is stored.

  ## Parameters
  - data_id: The ID of the data (argument or result)

  ## Returns
  - {:ok, node_id} if the data location is found
  - {:error, :not_found} if the data location is not registered
  """
  def lookup(data_id) do
    GenServer.call(__MODULE__, {:lookup, data_id})
  end

  @doc """
  Gets all registered data locations.

  ## Returns
  - A map of data_id => node_id
  """
  def get_all do
    GenServer.call(__MODULE__, :get_all)
  end

  @doc """
  Clears all registered data locations.
  """
  def clear do
    GenServer.call(__MODULE__, :clear)
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    # Initialize an empty registry
    {:ok, %{}}
  end

  @impl true
  def handle_call({:register, data_id, node_id}, _from, state) do
    Logger.debug("Registering data #{inspect(data_id)} at node #{inspect(node_id)}")
    {:reply, :ok, Map.put(state, data_id, node_id)}
  end

  @impl true
  def handle_call({:lookup, data_id}, _from, state) do
    case Map.fetch(state, data_id) do
      {:ok, node_id} -> {:reply, {:ok, node_id}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:get_all, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:clear, _from, _state) do
    {:reply, :ok, %{}}
  end
end
