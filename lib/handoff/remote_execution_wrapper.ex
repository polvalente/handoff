defmodule Handoff.RemoteExecutionWrapper do
  @moduledoc """
  Handles the execution of a function on a remote node,
  storing its result locally, and confirming back to the orchestrator.
  """

  alias Handoff.DataLocationRegistry
  alias Handoff.ResultStore

  require Logger

  @doc """
  Executes a function on this (remote) node, fetching its arguments from this node's
  local ResultStore, stores the result locally, and confirms back to the orchestrator.

  Resource management (request/release) is assumed to be handled by the orchestrator
  for the target node before this function is called.
  """
  def execute_and_store(dag_id, function_struct, arg_ids, orchestrator_node, all_dag_functions) do
    orchestrator = orchestrator_node || Node.self()

    if Node.self() != function_struct.node do
      Logger.warning(
        "RemoteExecutionWrapper executed on #{inspect(Node.self())} for function targeted at #{inspect(function_struct.node)}. This might indicate a misconfiguration or incorrect RPC target."
      )
    end

    try do
      # 1. Fetch arguments, handling inlines locally on this worker node.
      resolved_args = fetch_arguments(dag_id, arg_ids, orchestrator, all_dag_functions)

      # 2. Execute the function code
      actual_result =
        case function_struct.id do
          {:serialize, _, _} ->
            source_node = Node.self()
            target_node = function_struct.node

            apply_code(
              function_struct.code,
              resolved_args ++
                [source_node, target_node] ++
                function_struct.extra_args
            )

          {:deserialize, _, _} ->
            # For deserializer, source_node is producer's node, target_node is current node
            source_node = function_struct.node
            target_node = Node.self()

            apply_code(
              function_struct.code,
              resolved_args ++
                [source_node, target_node] ++
                function_struct.extra_args
            )

          _ ->
            apply_code(function_struct.code, resolved_args ++ function_struct.extra_args)
        end

      # 3. Store result in this node's local ResultStore
      case ResultStore.store(dag_id, function_struct.id, actual_result) do
        :ok ->
          # 4. Return success confirmation
          {:ok, :result_stored_locally}

        {:error, reason} ->
          Logger.error(
            "Failed to store result for dag_id #{inspect(dag_id)}, function #{inspect(function_struct.id)} on node #{inspect(Node.self())}: #{inspect(reason)}"
          )

          {:error, {:store_failed, reason}}
      end
    rescue
      e ->
        stacktrace = __STACKTRACE__

        Logger.error(
          "Error during remote execution of function #{inspect(function_struct.id)} for dag_id #{inspect(dag_id)} on node #{inspect(Node.self())}: #{inspect(e)}\n#{Exception.format_stacktrace(stacktrace)}"
        )

        {:error, {:execution_failed, Exception.format(:error, e, stacktrace)}}
    catch
      kind, reason ->
        stacktrace = __STACKTRACE__

        Logger.error(
          "#{kind} during remote execution of function #{inspect(function_struct.id)} for dag_id #{inspect(dag_id)} on node #{inspect(Node.self())}: #{inspect(reason)}\n#{Exception.format_stacktrace(stacktrace)}"
        )

        {:error, {kind, Exception.format(kind, reason, stacktrace)}}
    end
  end

  @doc false
  # Fetches arguments for the consumer function. If an argument is an inline function,
  # it's executed locally on this worker node (JIT).
  # Regular arguments are fetched from local ResultStore or via orchestrator/source node.
  defp fetch_arguments(dag_id, arg_ids_for_consumer, orchestrator, all_dag_functions) do
    Enum.map(arg_ids_for_consumer, fn arg_id ->
      # Check if arg_id refers to a defined function
      function_def = Map.get(all_dag_functions, arg_id)

      if function_def && function_def.type == :inline do
        # Inline function: execute it locally on this worker node.
        _execute_inline_on_worker(dag_id, function_def, orchestrator, all_dag_functions)
      else
        # Regular argument or initial literal: fetch it.
        # Check local store first for efficiency (might be a pre-cached regular arg)
        case ResultStore.get(dag_id, arg_id) do
          {:ok, value} ->
            value

          {:error, :not_found} ->
            # If not found locally, consult the orchestrator for this argument's location
            # This path is for regular, non-inline arguments.
            fetch_from_orchestrator(dag_id, arg_id, orchestrator)
        end
      end
    end)
  end

  # Helper: Executes an inline function locally on this worker node.
  defp _execute_inline_on_worker(dag_id, inline_function_def, orchestrator, all_dag_functions) do
    # Fetch arguments for the inline function itself.
    # These args might also be inline (leading to recursion) or regular.
    inline_args =
      fetch_arguments(
        dag_id,
        inline_function_def.args,
        orchestrator,
        all_dag_functions
      )

    # Execute the inline function
    apply_code(inline_function_def.code, inline_args ++ inline_function_def.extra_args)
  end

  # Helper: fetch an argument via the orchestrator
  defp fetch_from_orchestrator(dag_id, arg_id, orchestrator) do
    # This is a crucial step - ask the orchestrator where to find the data
    case :rpc.call(orchestrator, DataLocationRegistry, :lookup, [dag_id, arg_id]) do
      {:ok, source_node} ->
        # We know where the data is, now fetch it
        fetch_from_node(dag_id, arg_id, source_node)

      {:error, :not_found} ->
        # The orchestrator doesn't know where this data is - critical error
        raise "Orchestrator #{inspect(orchestrator)} has no location registered for #{inspect(arg_id)} in DAG #{inspect(dag_id)}"

      {:badrpc, reason} ->
        # RPC to orchestrator failed - can't proceed
        raise "Failed to contact orchestrator #{inspect(orchestrator)} to locate #{inspect(arg_id)} in DAG #{inspect(dag_id)}: #{inspect(reason)}"
    end
  end

  # Helper: fetch data from a specific node
  defp fetch_from_node(dag_id, arg_id, source_node) do
    if source_node == Node.self() do
      # This shouldn't happen (we already checked local store),
      # but handle it gracefully
      raise "Data inconsistency for #{inspect(arg_id)} in DAG #{inspect(dag_id)}: registry points to local node but data not found locally"
    else
      # Get from remote node
      case :rpc.call(source_node, ResultStore, :get, [dag_id, arg_id]) do
        {:ok, value} ->
          # Cache locally for future use
          ResultStore.store(dag_id, arg_id, value)
          value

        {:error, reason} ->
          raise "Failed to fetch #{inspect(arg_id)} for DAG #{inspect(dag_id)} from node #{inspect(source_node)}: #{inspect(reason)}"

        {:badrpc, reason} ->
          raise "RPC error fetching #{inspect(arg_id)} for DAG #{inspect(dag_id)} from node #{inspect(source_node)}: #{inspect(reason)}"
      end
    end
  end

  defp apply_code(function, args) when is_function(function) do
    apply(function, args)
  end

  defp apply_code({module, function}, args) do
    apply(module, function, args)
  end
end
