defmodule Handoff.Pipeline.Aggregator do
  @moduledoc """
  GenStage `:producer_consumer` that joins sink outputs and restores push order.

  Buffers per `correlation_id` until every sink has reported, then emits in
  monotonic `correlation_id` order (not arrival order).
  """

  use GenStage

  @doc false
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    sinks = Keyword.fetch!(opts, :sinks)

    subscribe_to =
      Enum.map(sinks, fn {pid, sink_id} ->
        {pid, max_demand: 10, min_demand: 1, sink_id: sink_id}
      end)

    sink_ids = Enum.map(sinks, fn {_pid, sink_id} -> sink_id end)

    state = %{
      sink_ids: sink_ids,
      single_sink?: match?([_], sink_ids),
      from_to_sink: %{},
      join: %{},
      completed: %{},
      next_cid: 0
    }

    {:producer_consumer, state, subscribe_to: subscribe_to}
  end

  @impl true
  def handle_subscribe(:producer, opts, from, state) do
    sink_id = Keyword.fetch!(opts, :sink_id)
    {:automatic, put_in(state.from_to_sink[from], sink_id)}
  end

  def handle_subscribe(:consumer, _opts, _from, state) do
    {:automatic, state}
  end

  @impl true
  def handle_events(events, from, state) do
    sink_id = Map.fetch!(state.from_to_sink, from)

    state =
      Enum.reduce(events, state, fn {cid, value}, st ->
        ingest(st, cid, sink_id, value)
      end)

    {emitted, state} = drain_ready(state, [])
    {:noreply, emitted, state}
  end

  defp ingest(state, cid, sink_id, value) do
    partial = Map.get(state.join, cid, %{})
    partial = Map.put(partial, sink_id, value)

    if Enum.all?(state.sink_ids, &Map.has_key?(partial, &1)) do
      output = format_output(state, partial)

      %{
        state
        | join: Map.delete(state.join, cid),
          completed: Map.put(state.completed, cid, output)
      }
    else
      %{state | join: Map.put(state.join, cid, partial)}
    end
  end

  defp format_output(%{single_sink?: true, sink_ids: [sink_id]}, partial) do
    Map.fetch!(partial, sink_id)
  end

  defp format_output(%{sink_ids: sink_ids}, partial) do
    Map.new(sink_ids, fn id -> {id, Map.fetch!(partial, id)} end)
  end

  defp drain_ready(%{next_cid: next, completed: completed} = state, acc) do
    case Map.pop(completed, next) do
      {nil, _} ->
        {Enum.reverse(acc), state}

      {value, completed} ->
        drain_ready(%{state | completed: completed, next_cid: next + 1}, [value | acc])
    end
  end
end
