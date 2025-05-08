defmodule Handoff.Telemetry do
  @moduledoc """
  Defines telemetry events and helpers for the Handoff library.

  This module contains:
  1. Constants for all telemetry event names
  2. Helper functions for emitting events
  3. Documentation about event measurements and metadata

  ## Event Naming Convention

  All Handoff events use the prefix `[:handoff]` followed by component and action:
  - `[:handoff, :executor, :function, :start]` - When a function execution starts
  - `[:handoff, :executor, :function, :stop]` - When a function execution completes
  - `[:handoff, :executor, :function, :exception]` - When a function execution fails

  ## Common Measurements and Metadata

  Every event includes appropriate measurements and metadata:
  - Start events include `:system_time` measurement
  - Stop events include `:duration` and `:system_time` measurements
  - Exception events include `:duration` and `:system_time` measurements
  - All events include function details in metadata

  ## Example Usage

  ```elixir
  :telemetry.attach(
    "my-handler-id",
    [:handoff, :executor, :function, :stop],
    fn event_name, measurements, metadata, config ->
      # Process the event
    end,
    nil
  )
  ```
  """

  # Event name constants
  # Executor events
  @executor_function_start [:handoff, :executor, :function, :start]
  @executor_function_stop [:handoff, :executor, :function, :stop]
  @executor_function_exception [:handoff, :executor, :function, :exception]

  # DAG events
  @dag_execution_start [:handoff, :dag, :execution, :start]
  @dag_execution_stop [:handoff, :dag, :execution, :stop]
  @dag_execution_exception [:handoff, :dag, :execution, :exception]

  # Resource tracker events
  @resource_request [:handoff, :resource_tracker, :request]
  @resource_allocation [:handoff, :resource_tracker, :allocation]
  @resource_release [:handoff, :resource_tracker, :release]

  # Allocator events
  @allocator_allocation_start [:handoff, :allocator, :allocation, :start]
  @allocator_allocation_stop [:handoff, :allocator, :allocation, :stop]

  # Event name getters
  def event_prefix, do: [:handoff]

  # Executor events
  def executor_function_start, do: @executor_function_start
  def executor_function_stop, do: @executor_function_stop
  def executor_function_exception, do: @executor_function_exception

  # DAG events
  def dag_execution_start, do: @dag_execution_start
  def dag_execution_stop, do: @dag_execution_stop
  def dag_execution_exception, do: @dag_execution_exception

  # Resource tracker events
  def resource_request, do: @resource_request
  def resource_allocation, do: @resource_allocation
  def resource_release, do: @resource_release

  # Allocator events
  def allocator_allocation_start, do: @allocator_allocation_start
  def allocator_allocation_stop, do: @allocator_allocation_stop

  @doc """
  Emits a telemetry event with the given name, measurements, and metadata.

  ## Parameters

  - `event_name` - The name of the event as a list of atoms
  - `measurements` - A map of measurements to include with the event
  - `metadata` - A map of metadata to include with the event
  """
  def emit_event(event_name, measurements, metadata) do
    :telemetry.execute(event_name, measurements, metadata)
  end

  @doc """
  Helper to emit start events with the current system time.
  """
  def emit_start_event(event_name, metadata) do
    emit_event(event_name, %{system_time: System.system_time()}, metadata)
  end

  @doc """
  Helper to emit stop events with duration and system time.
  """
  def emit_stop_event(event_name, start_time, metadata) do
    end_time = System.system_time()
    duration = end_time - start_time

    emit_event(
      event_name,
      %{
        duration: duration,
        system_time: end_time
      },
      metadata
    )
  end

  @doc """
  Helper to emit exception events with duration, system time, and exception details.
  """
  def emit_exception_event(event_name, start_time, exception, stacktrace, metadata) do
    end_time = System.system_time()
    duration = end_time - start_time

    exception_metadata =
      Map.merge(metadata, %{
        exception: exception,
        stacktrace: stacktrace
      })

    emit_event(
      event_name,
      %{
        duration: duration,
        system_time: end_time
      },
      exception_metadata
    )
  end
end
