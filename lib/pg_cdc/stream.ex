require Logger
require Postgrex.Messages

defmodule PgCdc.Stream do
  use Connection

  def start_link(options \\ []) do
    defaults = [
      host: "localhost",
      port: 5432,
      user: "postgres",
      database: nil,
      send_to: self()
    ]
    options = Keyword.merge(defaults, options)

    options[:database] || raise ArgumentError, ":database is required"

    Connection.start_link(__MODULE__, options)
  end

  def flushed(conn, lsn) do
    GenServer.cast(conn, {:flushed, lsn})
  end

  def init(options) do
    Process.send_after(self(), :wakeup, 1000)

    state = options |> Enum.into(%{}) |> Map.merge(%{
      conn: nil,
      buffer: "",
      messages: [],
      lsn_written: 0,
      lsn_flushed: 0,
      lsn_from_keepalive: 0,
      lsn_from_xlog: 0,
      last_status_send_at: nil
    })

    %{host: host, port: port, slot_name: slot_name} = state

    params = state
    |> Map.take([:user, :database])
    |> Map.put(:replication, "database")
    |> Enum.into(Keyword.new)

    with {:ok, conn} <- connect_socket(host, port),
      :ok <- handshake(conn, slot_name, params)
    do
      :inet.setopts(conn, active: :once)
      {:ok, %{state | conn: conn}}
    else
      {:error, error} ->
        Logger.warn "#{inspect error} (#{host}:#{port})"
        {:backoff, 2000, state}
    end
  end

  def disconnect(error, state) do
    Logger.warn "disconnected (#{error[:message]})"
    if state.conn do
      :gen_tcp.close(state.conn)
    end
    state = %{state | conn: nil, lsn_written: nil, lsn_flushed: nil, last_status_send_at: nil}
    {:backoff, 2000, state}
  end

  def handle_info({:tcp, socket, data}, %{buffer: buffer} = state) do
    {messages, buffer} = parse_messages(buffer <> data)

    state = process_messages(messages, state)

    if state.messages == [] do
      :inet.setopts(socket, active: :once) # Allow the socket to send us the next message
    end

    {:noreply, %{state | buffer: buffer}}
  end

  def handle_info({:tcp_closed, _socket}, state) do
    {:disconnect, [code: "", message: "connection closed"], state}
  end

  def handle_info(:wakeup, state) do
    Process.send_after(self(), :wakeup, 1000)
    if connected?(state) && time_to_send_status?(state) && have_status_to_send?(state) do
      {:noreply, send_status(state)}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:flushed, lsn}, state) do
    {:noreply, %{state | lsn_flushed: lsn}}
  end

  def handle_cast({:message, message}, state) do
    handle_message(message, state)
  end

  defp parse_messages(buffer, messages \\ []) do
    {message, buffer} = parse_message(buffer)
    if message do
      parse_messages(buffer, [message | messages])
    else
      {Enum.reverse(messages), buffer}
    end
  end

  defp process_messages(messages, state) do
    Enum.reduce(messages, state, fn message, state ->
      process_message(message, state)
    end)
  end

  defp process_message({:msg_copy_data, data} = message, state) do
    Logger.debug "<<- #{inspect message}"
    {messages, _} = Msg.CopyData.parse(data)
    process_messages(messages, state)
  end

  defp process_message({:msg_keepalive, msg} = message, state) do
    Logger.debug "<<- #{inspect message}"

    state = %{state | lsn_from_keepalive: msg.wal_end}

    if msg.urgent do
      send_status(state) # Force a status update.
    else
      state
    end
  end

  defp process_message({:msg_xlog_data, msg} = message, state) do
    Logger.debug "<<- #{inspect message}"
    %{state | messages: [msg | state.messages], lsn_from_xlog: msg.wal_end}
  end

  defp process_message(message, state) do
    Logger.debug "<<- (unhandled) #{inspect message}"
    state
  end

  defp handle_message({:msg_ready, :idle}, state) do
    %{conn: conn, slot_name: slot_name} = state
    Logger.debug "<<- msg_ready (idle)"

    wal_start = case state do
      %{start_at: start_at} ->
        use Bitwise
        hi = start_at >>> 32
        lo = start_at
        hi = <<hi::size(32)>> |> Base.encode16 |> String.replace(~r(^0+), "0")
        lo = <<lo::size(32)>> |> Base.encode16 |> String.replace(~r(^0+), "0")
        "#{hi}/#{lo}"
      _ ->
        "0/0"
    end

    message = Postgrex.Messages.msg_query(statement: "START_REPLICATION SLOT #{slot_name} LOGICAL #{wal_start};")
      |> Postgrex.Messages.encode_msg
    :ok = :gen_tcp.send(conn, message)
    {:noreply, state}
  end

  defp handle_message({:msg_copy_data, data}, state) do
    Logger.debug "<<- msg_copy_data #{inspect data}"
    {messages, _} = Msg.CopyData.parse(data)
    Enum.each messages, fn message ->
      GenServer.cast(self(), {:message, message})
    end
    {:noreply, state}
  end

  defp handle_message({:msg_keepalive, msg}, state) do
    Logger.debug "^^^ msg_keepalive #{inspect msg}"

    # Update lsn_written with keepalive info (if it's greater than what we got).
    # state = %{state | lsn_written: max(msg.wal_end, state.lsn_written || 0)}

    # Notify our listener so it can send a flushed message back to us?
    GenServer.cast(state.send_to, {:msg_keepalive, msg})

    if msg.urgent do
      {:noreply, send_status(state)} # Force a status update.
    else
      {:noreply, state}
    end
  end

  defp handle_message({:msg_xlog_data, msg}, state) do
    Logger.debug "^^^ msg_xlog_data #{inspect msg}"
    lsn_written = max(msg.wal_end, state.lsn_written || 0)
    GenServer.cast(state.send_to, {:msg_xlog_data, msg})
    {:noreply, %{state | lsn_written: lsn_written}}
  end

  defp handle_message({:msg_error, error}, state) do
    case error[:code] do
      "57P03" -> {:disconnect, error, state}
    end
  end

  defp handle_message(message, state) do
    Logger.debug "<<- unhandled message: #{inspect message}"
    {:noreply, state}
  end

  defp parse_message(""), do: {nil, ""}
  defp parse_message(buffer) do
    <<type::size(8), rest::binary>> = buffer
    case parse_message(type, rest) do
      nil -> {nil, buffer}
      {nil, _} -> {nil, buffer}
      response -> response
    end
  end

  defp parse_message(_, buffer) when byte_size(buffer) < 4, do: nil
  defp parse_message(type, buffer) do
    <<size::size(32), rest::binary>> = buffer
    payload_size = size - 4
    if byte_size(rest) >= payload_size do
      message = binary_part(rest, 0, payload_size)
      buffer = binary_part(rest, payload_size, byte_size(rest)-payload_size)
      message = Postgrex.Messages.parse(message, type, size)
      {message, buffer}
    else
      {nil, buffer}
    end
  end

  defp send_status(state) do
    %{
      lsn_written: lsn_written,
      lsn_flushed: lsn_flushed,
      lsn_from_keepalive: lsn_from_keepalive,
      lsn_from_xlog: lsn_from_xlog
    } = state

    # This is tricky. We want to use lsn from the keepalive message in some
    # cases. Basically because the lsn given in the keepalive can keep
    # increasing for some reason, even if nothing is going on. But if we have
    # unprocessed msg_xlog_data, we don't want to say we processed them!
    wal_written = if lsn_written < lsn_from_xlog do
      [lsn_written, lsn_from_keepalive]
      |> Enum.reject(& &1 == 0)
      |> Enum.min
    else
      Enum.max([lsn_written, lsn_from_keepalive])
    end

    msg_status = Msg.Status.new(wal_written: wal_written, wal_flushed: lsn_flushed)
    copy_data_payload = Msg.Status.serialize(msg_status)
    payload = Postgrex.Messages.msg_copy_data(data: copy_data_payload)
    |> Postgrex.Messages.encode_msg

    :ok = :gen_tcp.send(state.conn, payload)
    Logger.debug "->> #{inspect {:msg_status, Map.from_struct(msg_status)}}"

    %{state | last_status_send_at: now()}
  end

  defp connect_socket(host, port) do
    :gen_tcp.connect(String.to_char_list(host), port, [:binary, active: false])
  end

  defp handshake(conn, slot_name, params) do
    message = Postgrex.Messages.msg_startup(params: params)
    |> Postgrex.Messages.encode_msg

    message2 = Postgrex.Messages.msg_query(statement: "START_REPLICATION SLOT #{slot_name} LOGICAL 0/0;")
    |> Postgrex.Messages.encode_msg

    with :ok <- :gen_tcp.send(conn, message),
      :ok <- recv_messages_until_idle(conn),
      :ok <- :gen_tcp.send(conn, message2)
    do
      :ok
    end
  end

  defp recv_messages_until_idle(conn) do
    case read_message(conn) do
      {:msg_ready, :idle} = message ->
        Logger.debug "<<- #{inspect(message)}"
        :ok
      message ->
        Logger.debug "<<- unhandled message: #{inspect message}"
        recv_messages_until_idle(conn)
    end
  end

  defp read_message(conn) do
    import Postgrex.BinaryUtils
    with {:ok, <<type::int8>>} <- :gen_tcp.recv(conn, 1),
      {:ok, <<size::uint32>>} <- :gen_tcp.recv(conn, 4),
      {:ok, payload} <- :gen_tcp.recv(conn, size-4)
    do
      Postgrex.Messages.parse(payload, type, size)
    end
  end

  defp connected?(%{conn: nil}), do: false
  defp connected?(_), do: true

  defp time_to_send_status?(%{last_status_send_at: nil}), do: true
  defp time_to_send_status?(%{last_status_send_at: last}), do: now()-last >= 10000

  defp have_status_to_send?(%{lsn_written: 0, lsn_from_keepalive: 0}), do: false
  defp have_status_to_send?(_), do: true

  defp now(how \\ :millisecond), do: :os.system_time(how)

end
