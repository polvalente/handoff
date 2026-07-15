defmodule Handoff.Pipeline.Coordinator do
  @moduledoc """
  GenServer that owns a live GenStage pipeline compiled from a DAG.

  Builds one `InputStage` / `Stage` per node, wires subscriptions along
  `function.args` edges, assigns monotonic correlation ids on push, and
  tears the stage tree down on stop.
  """

  use GenServer

  alias Handoff.DAG
  alias Handoff.Pipeline.Aggregator
  alias Handoff.Pipeline.InputStage
  alias Handoff.Pipeline.Stage

  @doc false
  def start_link({dag, opts}) do
    GenServer.start_link(__MODULE__, {dag, opts})
  end

  @impl true
  def init({dag, _opts}) do
    Process.flag(:trap_exit, true)

    case DAG.validate(dag) do
      :ok ->
        case build_pipeline(dag) do
          {:ok, stages, aggregator, input_ids} ->
            {:ok,
             %{
               dag: dag,
               stages: stages,
               aggregator: aggregator,
               input_ids: input_ids,
               next_cid: 0
             }}

          {:error, reason} ->
            {:stop, reason}
        end

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:get_handle, _from, state) do
    handle = %Handoff.Pipeline{
      coordinator: self(),
      aggregator: state.aggregator,
      stages: state.stages
    }

    {:reply, handle, state}
  end

  def handle_call({:push, values}, _from, state) do
    case normalize_inputs(values, state.input_ids) do
      {:ok, input_map} ->
        cid = state.next_cid

        Enum.each(input_map, fn {input_id, value} ->
          pid = Map.fetch!(state.stages, input_id)
          InputStage.enqueue(pid, {cid, value})
        end)

        {:reply, {:ok, cid}, %{state | next_cid: cid + 1}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:EXIT, _pid, reason}, state) when reason in [:normal, :shutdown] do
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    {:stop, reason, state}
  end

  @impl true
  def terminate(_reason, state) do
    pids =
      Enum.filter([state.aggregator | Map.values(state.stages)], fn pid ->
        is_pid(pid) and Process.alive?(pid)
      end)

    Enum.each(pids, fn pid ->
      try do
        GenStage.stop(pid, :shutdown, 5_000)
      catch
        :exit, _ -> :ok
      end
    end)

    :ok
  end

  defp build_pipeline(dag) do
    input_ids =
      dag.functions
      |> Enum.filter(fn {_id, fun} -> fun.type == :input end)
      |> Enum.map(fn {id, _} -> id end)

    order = topological_order(dag)
    stages = %{}

    {stages, error} =
      Enum.reduce_while(order, {stages, nil}, fn id, {stages_acc, _} ->
        function = Map.fetch!(dag.functions, id)

        result =
          case function.type do
            :input ->
              InputStage.start_link(id: id)

            _ ->
              producers =
                Enum.map(function.args, fn dep_id ->
                  {Map.fetch!(stages_acc, dep_id), dep_id}
                end)

              Stage.start_link(function: function, producers: producers)
          end

        case result do
          {:ok, pid} ->
            Process.link(pid)
            {:cont, {Map.put(stages_acc, id, pid), nil}}

          {:error, reason} ->
            {:halt, {stages_acc, reason}}
        end
      end)

    if error do
      {:error, error}
    else
      sinks = sink_ids(dag)

      sink_producers =
        Enum.map(sinks, fn sink_id ->
          {Map.fetch!(stages, sink_id), sink_id}
        end)

      case Aggregator.start_link(sinks: sink_producers) do
        {:ok, aggregator} ->
          Process.link(aggregator)
          {:ok, stages, aggregator, input_ids}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp sink_ids(dag) do
    referenced =
      dag.functions
      |> Map.values()
      |> Enum.flat_map(& &1.args)
      |> MapSet.new()

    dag.functions
    |> Map.keys()
    |> Enum.reject(&MapSet.member?(referenced, &1))
  end

  defp topological_order(dag) do
    indegree =
      Map.new(dag.functions, fn {id, fun} -> {id, length(fun.args)} end)

    queue =
      indegree
      |> Enum.filter(fn {_id, d} -> d == 0 end)
      |> Enum.map(fn {id, _} -> id end)

    do_topo(queue, indegree, dag.functions, [])
  end

  defp do_topo([], indegree, _functions, acc) do
    if map_size(indegree) == 0 do
      Enum.reverse(acc)
    else
      # Should not happen after DAG.validate/1
      Enum.reverse(acc, Map.keys(indegree))
    end
  end

  defp do_topo([id | rest], indegree, functions, acc) do
    indegree = Map.delete(indegree, id)

    consumers =
      functions
      |> Enum.filter(fn {_cid, fun} -> id in fun.args end)
      |> Enum.map(fn {cid, _} -> cid end)

    {indegree, newly_ready} =
      Enum.reduce(consumers, {indegree, []}, fn consumer_id, {ind, ready} ->
        case Map.get(ind, consumer_id) do
          nil ->
            {ind, ready}

          d ->
            d = d - 1
            ind = Map.put(ind, consumer_id, d)
            if d == 0, do: {ind, [consumer_id | ready]}, else: {ind, ready}
        end
      end)

    do_topo(rest ++ Enum.reverse(newly_ready), indegree, functions, [id | acc])
  end

  defp normalize_inputs(values, [single_id]) when not is_map(values) do
    {:ok, %{single_id => values}}
  end

  defp normalize_inputs(values, input_ids) when is_map(values) do
    missing = Enum.reject(input_ids, &Map.has_key?(values, &1))

    if missing == [] do
      {:ok, Map.take(values, input_ids)}
    else
      {:error, {:missing_inputs, missing}}
    end
  end

  defp normalize_inputs(_values, input_ids) do
    {:error, {:expected_input_map, input_ids}}
  end
end
