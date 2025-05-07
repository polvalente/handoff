defmodule Handoff.Application do
  def start(_type, _args) do
    Supervisor.start_link(Handoff.Supervisor, [])
  end
end
