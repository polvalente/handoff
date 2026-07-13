defmodule ClassifierPipeline do
  @moduledoc false
  import Nx.Defn

  @serving_name ClassifierServing

  def collect_input(value), do: value

  def classify_data(value) do
    # Force local lookup: a bare atom falls back to distributed Serving discovery.
    result = Nx.Serving.batched_run({:local, @serving_name}, value)

    class =
      case result |> Nx.squeeze() |> Nx.to_number() do
        0 -> :even
        _ -> :odd
      end

    %{input: value, class: class, classified_on: Node.self()}
  end

  def show_output(result) do
    IO.puts(
      "input=#{result.input} class=#{result.class} classified_on=#{inspect(result.classified_on)}"
    )

    result
  end

  defn classify_tensor(x) do
    Nx.remainder(Nx.as_type(x, :s64), 2)
  end

  def client_preprocessing(value) when is_integer(value) do
    monitor_node = Application.fetch_env!(:classifier_demo, :monitor_node)
    send({:batch_monitor, monitor_node}, {:request, Node.self(), value})
    {Nx.Batch.stack([Nx.tensor([value])]), :ok}
  end

  def build_serving do
    Nx.Serving.new(fn opts ->
      fun = Nx.Defn.jit(&classify_tensor/1, opts)

      fn batch ->
        monitor_node = Application.fetch_env!(:classifier_demo, :monitor_node)

        send(
          {:batch_monitor, monitor_node},
          {:merged_batch, Node.self(), batch.size, inspect_batch(batch)}
        )

        # Give concurrent callers time to land in the same Serving batch.
        Process.sleep(80)
        fun.(batch)
      end
    end)
    |> Nx.Serving.client_preprocessing(&client_preprocessing/1)
    |> Nx.Serving.batch_size(5)
  end

  def start_serving(monitor_node) do
    Application.put_env(:classifier_demo, :monitor_node, monitor_node)

    case Process.whereis(@serving_name) do
      nil ->
        # start_link under :erpc would link to the short-lived RPC process and die with it.
        {:ok, pid} =
          Nx.Serving.start_link(
            serving: build_serving(),
            name: @serving_name,
            batch_size: 5,
            batch_timeout: 100
          )

        true = Process.unlink(pid)
        :ok

      _pid ->
        :ok
    end
  end

  def register_worker_resources(caps) do
    Handoff.register_node(Node.self(), caps)
  end

  def build_dag(i, orchestrator) do
    alias Handoff.Function

    {self(), i}
    |> Handoff.DAG.new()
    |> Handoff.DAG.add_function(%Function{
      id: :collect_input,
      args: [],
      code: &__MODULE__.collect_input/1,
      extra_args: [i],
      node: orchestrator
    })
    |> Handoff.DAG.add_function(%Function{
      id: :classify_data,
      args: [:collect_input],
      code: &__MODULE__.classify_data/1,
      cost: %{compute: 1}
    })
    |> Handoff.DAG.add_function(%Function{
      id: :show_output,
      args: [:classify_data],
      code: &__MODULE__.show_output/1,
      node: orchestrator
    })
  end

  defp inspect_batch(%Nx.Batch{size: size} = batch) do
    values =
      try do
        batch
        |> then(Nx.Defn.jit(fn b -> b end))
        |> Nx.to_flat_list()
      rescue
        _ -> :unavailable
      end

    %{size: size, values: values}
  end
end
