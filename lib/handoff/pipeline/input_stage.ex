defmodule Handoff.Pipeline.InputStage do
  @moduledoc """
  GenStage `:producer` for `type: :input` DAG nodes.

  Holds an internal queue of `{correlation_id, value}` events and satisfies
  demand from it. The pipeline coordinator enqueues items via `enqueue/2`.
  """

  use GenStage

  @doc false
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @doc """
  Enqueues a `{correlation_id, value}` event for later dispatch.
  """
  def enqueue(pid, {correlation_id, _value} = event)
      when is_pid(pid) and is_integer(correlation_id) do
    GenStage.cast(pid, {:enqueue, event})
  end

  @impl true
  def init(opts) do
    id = Keyword.fetch!(opts, :id)

    {:producer, %{id: id, queue: :queue.new(), demand: 0},
     dispatcher: GenStage.BroadcastDispatcher}
  end

  @impl true
  def handle_demand(incoming, state) when incoming > 0 do
    dispatch_events(%{state | demand: state.demand + incoming})
  end

  @impl true
  def handle_cast({:enqueue, event}, state) do
    dispatch_events(%{state | queue: :queue.in(event, state.queue)})
  end

  defp dispatch_events(%{demand: 0} = state) do
    {:noreply, [], state}
  end

  defp dispatch_events(%{queue: queue, demand: demand} = state) do
    {events, queue, demand} = take_events(queue, demand, [])
    {:noreply, Enum.reverse(events), %{state | queue: queue, demand: demand}}
  end

  defp take_events(queue, 0, acc), do: {acc, queue, 0}

  defp take_events(queue, demand, acc) do
    case :queue.out(queue) do
      {{:value, event}, queue} ->
        take_events(queue, demand - 1, [event | acc])

      {:empty, queue} ->
        {acc, queue, demand}
    end
  end
end
