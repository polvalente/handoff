defmodule Handout.Application do
  def start(_type, _args) do
    Supervisor.start_link(Handout.Supervisor, [])
  end
end
