defmodule Handout.DataLocationRegistry do
  @moduledoc """
  Tracks the location (node ID) of every piece of data (argument or intermediate result) for each DAG.

  This registry maintains a mapping of {dag_id, data_id} to their hosting nodes, enabling
  on-demand data fetching from the appropriate node for a specific DAG execution.
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
  Registers a data item with its hosting node for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - data_id: The ID of the data (argument or result)
  - node_id: The node where the data is stored
  """
  def register(dag_id, data_id, node_id) do
    GenServer.call(__MODULE__, {:register, dag_id, data_id, node_id})
  end

  @doc """
  Looks up where a data item is stored for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - data_id: The ID of the data (argument or result)

  ## Returns
  - {:ok, node_id} if the data location is found for the DAG
  - {:error, :not_found} if the data location is not registered for the DAG
  """
  def lookup(dag_id, data_id) do
    GenServer.call(__MODULE__, {:lookup, dag_id, data_id})
  end

  @doc """
  Gets all registered data locations for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG

  ## Returns
  - A map of data_id => node_id for the specified DAG
  """
  def get_all(dag_id) do
    GenServer.call(__MODULE__, {:get_all_for_dag, dag_id})
  end

  @doc """
  Clears all registered data locations for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  """
  def clear(dag_id) do
    GenServer.call(__MODULE__, {:clear_dag, dag_id})
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    # Initialize an empty registry (state is a map of {dag_id, data_id} => node_id)
    {:ok, %{}}
  end

  @impl true
  def handle_call({:register, dag_id, data_id, node_id}, _from, state) do
    Logger.debug(
      "Registering data #{inspect(data_id)} for DAG #{inspect(dag_id)} at node #{inspect(node_id)}"
    )

    {:reply, :ok, Map.put(state, {dag_id, data_id}, node_id)}
  end

  @impl true
  def handle_call({:lookup, dag_id, data_id}, _from, state) do
    case Map.fetch(state, {dag_id, data_id}) do
      {:ok, node_id} -> {:reply, {:ok, node_id}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:get_all_for_dag, dag_id}, _from, state) do
    dag_specific_entries =
      state
      |> Enum.filter(fn {{current_dag_id, _data_id}, _node_id} -> current_dag_id == dag_id end)
      |> Map.new(fn {{_dag_id, data_id}, node_id} -> {data_id, node_id} end)

    {:reply, dag_specific_entries, state}
  end

  @impl true
  def handle_call({:clear_dag, dag_id}, _from, state) do
    new_state =
      state
      |> Enum.reject(fn {{current_dag_id, _data_id}, _node_id} -> current_dag_id == dag_id end)
      |> Map.new()

    {:reply, :ok, new_state}
  end
end
