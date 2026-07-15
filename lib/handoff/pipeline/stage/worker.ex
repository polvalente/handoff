defmodule Handoff.Pipeline.Stage.Worker do
  @moduledoc false

  use GenServer

  alias Handoff.Pipeline.Invoke
  alias Handoff.Pipeline.Stage.Batch

  require Invoke

  @doc false
  def start_link(function) do
    GenServer.start_link(__MODULE__, function)
  end

  @doc false
  def start(function) do
    GenServer.start(__MODULE__, function)
  end

  @doc false
  def process_async(worker, stage, cid, args) do
    GenServer.cast(worker, {:process, stage, cid, args})
  end

  @impl true
  def init(function) do
    worker_state = run_init(function.init)

    {:ok,
     %{
       function: function,
       worker_state: worker_state,
       stage: nil,
       batch_buffer: [],
       batch_timer: nil
     }}
  end

  @impl true
  def handle_cast({:process, stage, cid, args}, state) do
    {:noreply, enqueue_batch(%{state | stage: stage}, cid, args)}
  end

  @impl true
  def handle_info({:batch_timeout, ref}, %{batch_timer: ref} = state) do
    {:noreply, flush_batch(%{state | batch_timer: nil})}
  end

  def handle_info({:batch_timeout, _stale}, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    cancel_batch_timer(state)
    :ok
  end

  defp enqueue_batch(state, cid, args) do
    buffer = state.batch_buffer ++ [{cid, args}]
    state = maybe_start_batch_timer(%{state | batch_buffer: buffer})

    if Batch.full?(state.function, state.batch_buffer) do
      flush_batch(state)
    else
      state
    end
  end

  defp maybe_start_batch_timer(%{batch_timer: nil, function: %{batch_timeout: timeout}} = state)
       when is_integer(timeout) do
    ref = make_ref()
    Process.send_after(self(), {:batch_timeout, ref}, timeout)
    %{state | batch_timer: ref}
  end

  defp maybe_start_batch_timer(state), do: state

  defp flush_batch(%{batch_buffer: []} = state), do: state

  defp flush_batch(state) do
    state = cancel_batch_timer(state)
    items = state.batch_buffer
    state = %{state | batch_buffer: []}

    case Invoke.safe(do: Batch.invoke_and_unbatch(state.function, state.worker_state, items)) do
      {:ok, {pairs, worker_state}} ->
        send(state.stage, {:worker_results, pairs})
        %{state | worker_state: worker_state}

      {:error, reason} ->
        pairs = Enum.map(items, fn {cid, _} -> {cid, {:error, reason}} end)
        send(state.stage, {:worker_results, pairs})
        state
    end
  end

  defp cancel_batch_timer(%{batch_timer: nil} = state), do: state

  defp cancel_batch_timer(%{batch_timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | batch_timer: nil}
  end

  defp run_init(nil), do: nil

  defp run_init({module, fun, args}) when is_atom(module) and is_atom(fun) and is_list(args) do
    apply(module, fun, args)
  end

  defp run_init(fun) when is_function(fun, 0), do: fun.()

  defp run_init(fun) when is_function(fun) do
    raise ":init named capture must have arity 0, got: #{inspect(fun)}"
  end
end
