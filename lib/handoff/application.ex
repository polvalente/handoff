defmodule Handoff.Application do
  @moduledoc false
  def start(_type, _args) do
    resource_tracker =
      Application.get_env(:handoff, :resource_tracker, Handoff.SimpleResourceTracker)

    Supervisor.start_link(Handoff.Supervisor, resource_tracker: resource_tracker)
  end
end
