defmodule Handout.Logger do
  @moduledoc """
  Standardized logging for Handout operations.

  This module provides consistent log formatting and structure across all Handout components.
  It wraps Elixir's Logger for consistent formatting and metadata handling.

  ## Logging Levels

  - `:debug` - Detailed debugging information
  - `:info` - Normal operational information
  - `:warning` - Non-critical issues that may affect performance
  - `:error` - Critical failures that prevent operation

  ## Standard Metadata

  All logs include standard metadata where appropriate:
  - `:node` - The node where the operation is occurring
  - `:module` - The module generating the log
  - `:function_id` - The ID of the function being executed
  - `:dag_id` - The ID of the DAG being processed

  ## Example Usage

  ```elixir
  Handout.Logger.info("Function execution started",
    function_id: function.id,
    node: Node.self())
  ```
  """

  require Logger

  @doc """
  Logs a debug message with standard metadata.
  """
  def debug(message, metadata \\ []) do
    Logger.debug(message, add_standard_metadata(metadata))
  end

  @doc """
  Logs an info message with standard metadata.
  """
  def info(message, metadata \\ []) do
    Logger.info(message, add_standard_metadata(metadata))
  end

  @doc """
  Logs a warning message with standard metadata.
  """
  def warning(message, metadata \\ []) do
    Logger.warning(message, add_standard_metadata(metadata))
  end

  @doc """
  Logs an error message with standard metadata.
  """
  def error(message, metadata \\ []) do
    Logger.error(message, add_standard_metadata(metadata))
  end

  @doc """
  Logs a function execution with standard timing information.
  """
  def log_function_execution(level, function_id, start_time, result, metadata \\ []) do
    end_time = System.system_time(:millisecond)
    duration_ms = end_time - start_time

    status = if match?({:ok, _}, result), do: "completed", else: "failed"

    message = "Function #{function_id} #{status} in #{duration_ms}ms"

    result_metadata =
      case result do
        {:ok, value} -> [{:result_type, :ok} | metadata]
        {:error, reason} -> [{:result_type, :error}, {:error_reason, reason} | metadata]
      end

    execution_metadata = [
      {:function_id, function_id},
      {:duration_ms, duration_ms}
      | result_metadata
    ]

    case level do
      :debug -> debug(message, execution_metadata)
      :info -> info(message, execution_metadata)
      :warning -> warning(message, execution_metadata)
      :error -> error(message, execution_metadata)
    end
  end

  @doc """
  Adds standard metadata to the provided metadata keyword list.
  """
  def add_standard_metadata(metadata) do
    default_metadata = [
      node: Node.self(),
      application: :handout
    ]

    Keyword.merge(default_metadata, metadata)
  end
end
