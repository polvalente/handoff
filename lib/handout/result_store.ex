defmodule Handout.ResultStore do
  @moduledoc """
  Provides storage and retrieval of function execution results.

  Maintains an ETS table for fast access to results by function ID.
  """

  use GenServer

  @table_name :handout_results

  # Client API

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Stores the result of a function execution.

  ## Parameters
  - function_id: The ID of the function whose result is being stored
  - result: The result value to store
  """
  def store(function_id, result) do
    :ets.insert(@table_name, {function_id, result})
    :ok
  end

  @doc """
  Retrieves the result for a function.

  ## Parameters
  - function_id: The ID of the function whose result to retrieve

  ## Returns
  - {:ok, result} if the result is found
  - {:error, :not_found} if no result exists for the function
  """
  def get(function_id) do
    case :ets.lookup(@table_name, function_id) do
      [{^function_id, result}] -> {:ok, result}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Checks if a result exists for the given function ID.

  ## Parameters
  - function_id: The ID of the function to check

  ## Returns
  - true if a result exists
  - false otherwise
  """
  def has_result?(function_id) do
    :ets.member(@table_name, function_id)
  end

  @doc """
  Clears all stored results.
  """
  def clear do
    :ets.delete_all_objects(@table_name)
    :ok
  end

  # Server callbacks

  @impl true
  def init(_) do
    table = :ets.new(@table_name, [:set, :named_table, :public, read_concurrency: true])
    {:ok, %{table: table}}
  end
end
