defmodule Handoff.Pipeline.Stage do
  @moduledoc """
  GenStage `:producer_consumer` for a single DAG node in a streaming pipeline.

  Runs `:init` once on start to build persistent worker state, joins fan-in
  inputs by `correlation_id`, and invokes `:code` once per ready item.
  """

  use GenStage

  @doc false
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    function = Keyword.fetch!(opts, :function)
    producers = Keyword.get(opts, :producers, [])

    worker_state = run_init(function.init)

    subscribe_to =
      Enum.map(producers, fn {pid, dep_id} ->
        {pid, max_demand: 10, min_demand: 1, dep_id: dep_id}
      end)

    state = %{
      function: function,
      worker_state: worker_state,
      join: %{},
      from_to_dep: %{}
    }

    {:producer_consumer, state, dispatcher: GenStage.BroadcastDispatcher,
     subscribe_to: subscribe_to}
  end

  @impl true
  def handle_subscribe(:producer, opts, from, state) do
    dep_id = Keyword.fetch!(opts, :dep_id)
    {:automatic, put_in(state.from_to_dep[from], dep_id)}
  end

  def handle_subscribe(:consumer, _opts, _from, state) do
    {:automatic, state}
  end

  @impl true
  def handle_events(events, from, state) do
    dep_id = Map.fetch!(state.from_to_dep, from)

    {emitted, state} =
      Enum.reduce(events, {[], state}, fn {cid, value}, {acc, st} ->
        {outs, st} = ingest(st, cid, dep_id, value)
        {acc ++ outs, st}
      end)

    {:noreply, emitted, state}
  end

  defp ingest(state, cid, dep_id, value) do
    partial = Map.get(state.join, cid, %{})
    partial = Map.put(partial, dep_id, value)

    if ready?(state.function.args, partial) do
      args = Enum.map(state.function.args, &Map.fetch!(partial, &1))
      {result, worker_state} = invoke(state.function, state.worker_state, args)
      state = %{state | join: Map.delete(state.join, cid), worker_state: worker_state}
      {[{cid, result}], state}
    else
      {[], %{state | join: Map.put(state.join, cid, partial)}}
    end
  end

  defp ready?(args, partial) do
    Enum.all?(args, &Map.has_key?(partial, &1))
  end

  defp invoke(%{init: nil} = function, _worker_state, args) do
    call_args =
      case function.argument_inclusion do
        :variadic -> args ++ function.extra_args
        :as_list -> [args | function.extra_args]
      end

    {apply_code(function.code, call_args), nil}
  end

  defp invoke(function, worker_state, args) do
    call_args =
      case function.argument_inclusion do
        :variadic -> [worker_state | args] ++ function.extra_args
        :as_list -> [worker_state, args | function.extra_args]
      end

    case apply_code(function.code, call_args) do
      {result, new_state} ->
        {result, new_state}

      other ->
        raise "streaming :code with :init set must return {result, new_state}, got: #{inspect(other)}"
    end
  end

  defp run_init(nil), do: nil

  defp run_init({module, fun, args}) when is_atom(module) and is_atom(fun) and is_list(args) do
    apply(module, fun, args)
  end

  defp run_init(fun) when is_function(fun, 0), do: fun.()

  defp run_init(fun) when is_function(fun) do
    raise ":init named capture must have arity 0, got: #{inspect(fun)}"
  end

  defp apply_code(code, args) when is_function(code), do: apply(code, args)
  defp apply_code({module, fun}, args), do: apply(module, fun, args)
end
