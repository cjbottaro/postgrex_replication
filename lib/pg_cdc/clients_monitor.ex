require Logger

defmodule PgCdc.ClientsMonitor do
  use GenServer

  defstruct [conn: nil, clients: MapSet.new]

  import Map, only: [put: 3]

  alias PgCdc.Supervisor

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_args) do
    conn_info = Application.fetch_env!(:pg_cdc, :clients_db)
    {:ok, conn} = Postgrex.start_link(conn_info)
    send(self(), :check)
    {:ok, %__MODULE__{conn: conn}}
  end

  def handle_info(:check, state) do
    %{conn: conn, clients: clients} = state

    all_clients = Postgrex.query!(conn, "SELECT name,shard_host FROM clients", [])
      |> to_map
      |> Enum.map(fn row -> {row.name, row.shard_host} end)
      |> MapSet.new

    new_clients = MapSet.difference(all_clients, clients)

    if MapSet.size(new_clients) > 0 do
      new_client_names = Enum.map(new_clients, fn {name, _} -> name end)
      Logger.info "New clients detected: #{inspect new_client_names}"
      Enum.each new_clients, fn {client, shard_host} ->
        Supervisor.Database.start({client, shard_host})
      end
    end
    clients = MapSet.union(clients, new_clients)

    Process.send_after(self(), :check, 2000)

    {:noreply, put(state, :clients, clients)}
  end

  defp to_map(result) do
    %{columns: columns, rows: rows} = result
    columns = Enum.map(columns, &String.to_atom/1)
    Enum.map(rows, fn row ->
      Enum.zip(columns, row) |> Enum.into(%{})
    end)
  end

end
