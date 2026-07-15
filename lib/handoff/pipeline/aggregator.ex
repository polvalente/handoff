defmodule Handoff.Pipeline.Aggregator do
  @moduledoc """
  GenStage `:producer_consumer` that joins sink outputs and restores push order.

  Buffers per `correlation_id` until every sink has reported, then emits in
  monotonic `correlation_id` order (not arrival order).

  Mid-pipeline `:code` failures are reported directly via
  `{:item_error, cid, reason}` (not via hop-by-hop GenStage events). One-hop
  `{:suppress, reason}` events from sinks are ignored here. When `:join_timeout`
  is set, incomplete multi-sink joins are completed as `{:error, :join_timeout}`.

  Subscription demand defaults to `max_demand: 10` / `min_demand: 1`.
  """

  use GenStage

  @doc false
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    sinks = Keyword.fetch!(opts, :sinks)
    join_timeout = Keyword.get(opts, :join_timeout)
    max_demand = Keyword.get(opts, :max_demand, 10)
    min_demand = Keyword.get(opts, :min_demand, 1)

    subscribe_to =
      Enum.map(sinks, fn {pid, sink_id} ->
        {pid, max_demand: max_demand, min_demand: min_demand, sink_id: sink_id}
      end)

    sink_ids = Enum.map(sinks, fn {_pid, sink_id} -> sink_id end)

    state = %{
      sink_ids: sink_ids,
      single_sink?: match?([_], sink_ids),
      from_to_sink: %{},
      join: %{},
      join_timers: %{},
      join_timeout: join_timeout,
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
      Enum.reduce(events, state, fn
        {_cid, {:suppress, _}}, st ->
          st

        {cid, value}, st ->
          ingest(st, cid, sink_id, value)
      end)

    {emitted, state} = drain_ready(state, [])
    {:noreply, emitted, state}
  end

  @impl true
  def handle_info({:item_error, cid, reason}, state) do
    state = complete_with(state, cid, {:error, reason})
    {emitted, state} = drain_ready(state, [])
    {:noreply, emitted, state}
  end

  def handle_info({:join_timeout, cid}, state) do
    case Map.pop(state.join, cid) do
      {nil, _} ->
        {:noreply, [], state}

      {_partial, join} ->
        state = cancel_join_timer(state, cid)
        state = %{state | join: Map.put(join, cid, :timed_out), completed: Map.put(state.completed, cid, {:error, :join_timeout})}
        state = schedule_join_gc(state, cid)
        {emitted, state} = drain_ready(state, [])
        {:noreply, emitted, state}
    end
  end

  def handle_info({:gc_join, cid}, state) do
    state =
      case Map.get(state.join, cid) do
        :timed_out ->
          %{state | join: Map.delete(state.join, cid)}

        _ ->
          state
      end

    {:noreply, [], state}
  end

  defp ingest(state, cid, sink_id, value) do
    cond do
      cid < state.next_cid ->
        state

      Map.has_key?(state.completed, cid) ->
        state

      Map.get(state.join, cid) == :timed_out ->
        state

      true ->
        do_ingest(state, cid, sink_id, value)
    end
  end

  defp complete_with(state, cid, output) do
    cond do
      cid < state.next_cid ->
        state

      Map.has_key?(state.completed, cid) ->
        state

      true ->
        state = clear_join(state, cid)
        %{state | completed: Map.put(state.completed, cid, output)}
    end
  end

  defp do_ingest(state, cid, sink_id, value) do
    partial = Map.get(state.join, cid) || %{}
    first? = partial == %{}
    partial = Map.put(partial, sink_id, value)

    state =
      if first? do
        maybe_arm_join_timer(state, cid)
      else
        state
      end

    if Enum.all?(state.sink_ids, &Map.has_key?(partial, &1)) do
      output = format_output(state, partial)

      state
      |> clear_join(cid)
      |> then(fn st -> %{st | completed: Map.put(st.completed, cid, output)} end)
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

  defp maybe_arm_join_timer(%{join_timeout: timeout} = state, cid) when is_integer(timeout) do
    ref = Process.send_after(self(), {:join_timeout, cid}, timeout)
    %{state | join_timers: Map.put(state.join_timers, cid, ref)}
  end

  defp maybe_arm_join_timer(state, _cid), do: state

  defp schedule_join_gc(%{join_timeout: timeout} = state, cid) when is_integer(timeout) do
    Process.send_after(self(), {:gc_join, cid}, timeout)
    state
  end

  defp schedule_join_gc(state, _cid), do: state

  defp clear_join(state, cid) do
    state
    |> cancel_join_timer(cid)
    |> then(fn st -> %{st | join: Map.delete(st.join, cid)} end)
  end

  defp cancel_join_timer(state, cid) do
    case Map.pop(state.join_timers, cid) do
      {nil, _} ->
        state

      {ref, timers} ->
        Process.cancel_timer(ref)
        %{state | join_timers: timers}
    end
  end
end
