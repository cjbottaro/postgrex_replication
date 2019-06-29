defmodule Msg.Status do
  defstruct [wal_written: 0, wal_flushed: 0, wal_applied: 0, urgent: 0]

  def new(attributes \\ []) do
    struct!(__MODULE__, attributes)
  end

  def serialize(status_update) do
    %{
      wal_written: wal_written,
      wal_flushed: wal_flushed,
      wal_applied: wal_applied,
      urgent: urgent
    } = status_update
    timestamp = :os.system_time(:microsecond)
    <<
      ?r::size(8),
      wal_written::size(64),
      wal_flushed::size(64),
      wal_applied::size(64),
      timestamp::size(64),
      urgent::size(8)
    >>
  end

end
