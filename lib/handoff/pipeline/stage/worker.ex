defmodule Handoff.Pipeline.Stage.Worker do
  @moduledoc false

  use GenServer

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
    {:ok, %{function: function, worker_state: worker_state}}
  end

  @impl true
  def handle_cast({:process, stage, cid, args}, state) do
    {result, worker_state} = invoke(state.function, state.worker_state, args)
    send(stage, {:worker_result, cid, result})
    {:noreply, %{state | worker_state: worker_state}}
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
