defmodule Msg.CopyData do

  def parse(buffer, messages \\ []) do
    case parse_single(buffer) do
      {nil, buffer} -> {Enum.reverse(messages), buffer}
      {message, buffer} -> parse(buffer, [message | messages])
    end
  end

  defp parse_single(""), do: {nil, ""}

  defp parse_single(buffer) do
    <<type::size(8), rest::binary>> = buffer
    parse_single(type, rest)
  end

  defp parse_single(?k, buffer) do
    <<wal_end::size(64), timestamp::size(64), urgent::size(8), rest::binary>> = buffer
    message = %{wal_end: wal_end, timestamp: timestamp, urgent: urgent != 0}
    {{:msg_keepalive, message}, rest}
  end

  defp parse_single(?w, buffer) do
    <<wal_start::size(64), wal_end::size(64), timestamp::size(64), data::binary>> = buffer
    msg = %{wal_start: wal_start, wal_end: wal_end, timestamp: timestamp, data: data}
    {{:msg_xlog_data, msg}, ""}
  end

end
