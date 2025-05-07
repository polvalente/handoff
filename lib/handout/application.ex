defmodule Handoff.Application do
  @moduledoc false
  def start(_type, _args) do
    Supervisor.start_link(Handoff.Supervisor, [])
  end
end
