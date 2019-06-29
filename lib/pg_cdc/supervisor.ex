defmodule PgCdc.Supervisor do
  use Supervisor

  def start_link do
    Supervisor.start_link(PgCdc.Supervisor, [], name: __MODULE__)
  end

  def init(_args) do
    children = [
      worker(PgCdc.ClientsMonitor, []),
      supervisor(PgCdc.Supervisor.Database, [])
    ]
    supervise(children, strategy: :one_for_one)
  end

end
