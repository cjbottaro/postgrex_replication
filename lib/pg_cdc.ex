defmodule PgCdc do
  use Application

  def start(_type, _args) do
    PgCdc.Supervisor.start_link
  end
end
