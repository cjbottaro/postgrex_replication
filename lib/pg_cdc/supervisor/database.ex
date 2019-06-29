require Logger

defmodule PgCdc.Supervisor.Database do
  use Supervisor

  @name __MODULE__
  @ets_name :databases_ets

  def start({client, host}) do
    case :ets.lookup(@ets_name, client) do
      [{^client, _pid}] ->
        Logger.warn "Already monitoring #{client}"
      [] ->
        {:ok, pid} = Supervisor.start_child(@name, [{client, host}])
        :ets.insert(@ets_name, {client, pid})
    end
  end

  def start_link do
    Supervisor.start_link(__MODULE__, [], name: @name)
  end

  def init(_args) do
    :ets.new(@ets_name, [:named_table, :public])

    children = [
      worker(PgCdc.DatabaseMonitor, [], restart: :transient)
    ]

    supervise(children, strategy: :simple_one_for_one)
  end

end
