defmodule Handoff.FunctionRunner do
  @moduledoc """
  Executes individual DAG functions locally or remotely.

  Returns outcomes to the caller (`Handoff.DAGRunner`). Does not manage
  resource claims/releases or GenServer replies — those belong to the runner.
  """

  alias Handoff.Allocator.AllocationError
  alias Handoff.DataLocationRegistry
  alias Handoff.ResultStore

  require Logger

  @doc """
  Executes a function with retries.

  ## Returns
  - `{:ok, result}` for successful local execution
  - `{:ok, {:remote_store_and_registry_ok, function_id, node}}` for remote success
  - `{:error, reason}` after retries are exhausted
  """
  def execute(dag_id, function, args, max_retries, all_dag_functions, current_retry \\ 0) do
    case execute_with_node_type(dag_id, function, args, all_dag_functions) do
      {:ok, result} ->
        {:ok, result}

      {:error, :resources_unavailable} ->
        raise AllocationError,
              "Resources unavailable for function #{inspect(function.id)} on node #{function.node}"

      {:error, reason} when current_retry < max_retries ->
        Logger.warning(
          "Retrying function #{inspect(function.id)} execution (attempt #{current_retry + 1}/#{max_retries + 1}): #{inspect(reason)}"
        )

        execute(dag_id, function, args, max_retries, all_dag_functions, current_retry + 1)

      {:error, reason} ->
        {:error,
         "Failed to execute function #{inspect(function.id)} after #{max_retries + 1} attempts. Last error: #{inspect(reason)}"}
    end
  rescue
    e in [AllocationError] ->
      reraise e, __STACKTRACE__

    e ->
      if current_retry < max_retries do
        Logger.warning(
          "Retrying function #{inspect(function.id)} after error (attempt #{current_retry + 1}/#{max_retries + 1}): #{inspect(e)}"
        )

        execute(dag_id, function, args, max_retries, all_dag_functions, current_retry + 1)
      else
        {:error, "Function #{inspect(function.id)} failed with exception: #{inspect(e)}"}
      end
  end

  @doc """
  Resolves arguments for a function about to execute on `target_node`.
  """
  def fetch_arguments(dag_id, arg_ids, executed_results, target_node, all_dag_functions) do
    if target_node == Node.self() or target_node == nil do
      Enum.map(
        arg_ids,
        &resolve_argument(&1, dag_id, executed_results, target_node, all_dag_functions)
      )
    else
      # Remote execution: pass arg_ids; RemoteExecutionWrapper fetches/inlines.
      arg_ids
    end
  end

  defp execute_with_node_type(dag_id, function, args, all_dag_functions) do
    if !function.node || function.node == Node.self() do
      execute_local(function, args, all_dag_functions)
    else
      execute_remote(dag_id, function, args, all_dag_functions)
    end
  end

  defp execute_local(function, args, all_dag_functions) do
    args =
      case function.argument_inclusion do
        :variadic -> args
        :as_list -> [args]
      end

    case function.id do
      {:serialize, _unique_id, producer_id, consumer_id, _args} ->
        producer_function = Map.fetch!(all_dag_functions, producer_id)
        consumer_function = Map.fetch!(all_dag_functions, consumer_id)
        source_node = producer_function.node
        target_node = consumer_function.node

        result =
          apply_code(function.code, args ++ [source_node, target_node] ++ function.extra_args)

        {:ok, result}

      {:deserialize, _unique_id, producer_id, consumer_id, _args} ->
        producer_function = Map.fetch!(all_dag_functions, producer_id)
        consumer_function = Map.fetch!(all_dag_functions, consumer_id)
        source_node = producer_function.node
        target_node = consumer_function.node

        result =
          apply_code(function.code, args ++ [source_node, target_node] ++ function.extra_args)

        {:ok, result}

      _ ->
        result = apply_code(function.code, args ++ function.extra_args)
        {:ok, result}
    end
  end

  defp execute_remote(dag_id, function, args, all_dag_functions) do
    case :rpc.call(function.node, Handoff.RemoteExecutionWrapper, :execute_and_store, [
           dag_id,
           function,
           args,
           Node.self(),
           all_dag_functions
         ]) do
      {:ok, :result_stored_locally} ->
        DataLocationRegistry.register(dag_id, function.id, function.node)
        {:ok, {:remote_store_and_registry_ok, function.id, function.node}}

      {:error, reason} ->
        {:error, reason}

      {:badrpc, reason} ->
        {:error, reason}
    end
  end

  defp execute_inline_local(dag_id, inline_function_def, executed_results, all_dag_functions) do
    inline_args =
      fetch_arguments(
        dag_id,
        inline_function_def.args,
        executed_results,
        Node.self(),
        all_dag_functions
      )

    apply_code(inline_function_def.code, inline_args ++ inline_function_def.extra_args)
  end

  defp resolve_argument(arg_id, dag_id, executed_results, target_node, all_dag_functions) do
    function_def = Map.get(all_dag_functions, arg_id)
    result = Map.get(executed_results, arg_id)

    cond do
      function_def && function_def.type == :inline ->
        execute_inline_local(dag_id, function_def, executed_results, all_dag_functions)

      result == :remote_executed_and_registered ->
        with {:ok, source_node} <- DataLocationRegistry.lookup(dag_id, arg_id),
             {:ok, actual_value} <- :rpc.call(source_node, ResultStore, :get, [dag_id, arg_id]) do
          ResultStore.store(dag_id, arg_id, actual_value)
          actual_value
        else
          {:error, :not_found} ->
            raise "No location registered for remote arg_id #{inspect(arg_id)}"

          {:error, reason} ->
            raise "Failed to fetch remote result for arg_id #{inspect(arg_id)}: #{inspect(reason)}"

          {:badrpc, reason} ->
            raise "RPC error fetching result for arg_id #{inspect(arg_id)}: #{inspect(reason)}"
        end

      result ->
        result

      true ->
        get_with_fetch(dag_id, arg_id, target_node)
    end
  end

  defp get_with_fetch(dag_id, arg_id, target_node) do
    case ResultStore.get_with_fetch(dag_id, arg_id) do
      {:ok, value} ->
        value

      {:error, _reason} ->
        raise RuntimeError,
          message:
            "Orchestrator failed to fetch argument #{inspect(arg_id)} for DAG #{inspect(dag_id)} for local execution on target_node #{inspect(target_node)}"
    end
  end

  defp apply_code(function, args) when is_function(function) do
    apply(function, args)
  end

  defp apply_code({module, function}, args) do
    apply(module, function, args)
  end
end
