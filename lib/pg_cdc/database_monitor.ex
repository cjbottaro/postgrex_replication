require Logger

defmodule PgCdc.DatabaseMonitor do
  use GenServer

  @decoder_plugin "test_decoding"

  defstruct [:repl_conn, :nats_conn, :client, :host, :database, :slot_name]

  def start_link({client, shard_host}) do
    GenServer.start_link(__MODULE__, {client, shard_host})
  end

  def init({client, shard_host}) do
    database = database_from_client(client)
    Logger.info "[#{database}] initializing database monitor"

    GenServer.cast(self(), :setup)

    state = %__MODULE__{
      client: client,
      database: database,
      host: shard_host,
      slot_name: slot_name_from_client(client)
    }

    {:ok, state}
  end

  def handle_cast(:setup, state) do
    import Enum, only: [empty?: 1]

    %{database: database, host: host, slot_name: slot_name} = state

    # Connect to database.
    conn_info = Application.fetch_env!(:pg_cdc, :clients_db)
    conn_info = Keyword.put(conn_info, :host, host)
    conn_info = Keyword.put(conn_info, :database, database)
    {:ok, conn} = Postgrex.start_link(conn_info)

    # Check to see if replication slot exists.
    query = """
      SELECT slot_name
        FROM pg_replication_slots
       WHERE database = $1
         AND slot_name = $2
    """
    %{rows: rows} = Postgrex.query!(conn, query, [database, slot_name])

    # If it doesn't exist, create it.
    if empty?(rows) do
      query = "SELECT 1 FROM pg_create_logical_replication_slot($1, $2)"
      %Postgrex.Result{} = Postgrex.query!(conn, query, [slot_name, @decoder_plugin])
      Logger.info "[#{database}] logical replication slot created"
    end

    # Close the connection
    GenServer.stop(conn)

    # Startup the streaming replication connection.
    conn_info = Keyword.put(conn_info, :slot_name, slot_name)
    {:ok, repl_conn} = PgCdc.Stream.start_link(conn_info)

    # Startup the NATS Streaming connection.
    nats_conn_info = Application.fetch_env!(:pg_cdc, :nats)
      |> Keyword.put(:client_id, "pg_cdc_#{state.client}")
    {:ok, nats_conn} = Gnat.Stream.start_link(nats_conn_info)

    {:noreply, %{state | repl_conn: repl_conn, nats_conn: nats_conn}}
  end

  def handle_cast({:msg_xlog_data, msg}, state) do
    %{nats_conn: nats_conn, client: client} = state

    # Write to the nats server.
    payload = %{client: client, data: msg.data} |> Poison.encode!
    Gnat.Stream.publish(nats_conn, "pg_cdc", payload)

    # Notify the replication connection that we've flushed this data.
    PgCdc.Stream.flushed(state.repl_conn, msg.wal_end)

    {:noreply, state}
  end

  def handle_cast({:msg_keepalive, msg}, state) do
    # Notify the replication connection that we've flushed this data.
    PgCdc.Stream.flushed(state.repl_conn, msg.wal_end)
    {:noreply, state}
  end

  defp database_from_client(client) do
    client = String.replace(client, "-", "_")
    "shard_#{client}_#{env()}_master"
  end

  defp env do
    case Mix.env do
      :dev -> "development"
    end
  end

  defp slot_name_from_client(client) do
    client = String.replace(client, "-", "_")
    "slot_#{client}"
  end

end
