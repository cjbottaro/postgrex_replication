defmodule PgCdc.AttributeParser do

  def parse(string) do
    {column, type, string} = parse_column_and_type(string)
    {value, string} = parse_null_or_value(string, type)

    item = {column, type, value}
    {item, String.trim_leading(string)}
  end

  def parse_regex(string, regex) do
    [total | matches] = Regex.run(regex, string)
    i = String.length(total)
    string = String.slice(string, i..-1)
    {matches, string}
  end

  defp parse_column_and_type(string) do
    {matches, string} = parse_regex(string, ~r/(\w+)\[([\w ]+)\]:/)
    [column, type | _] = matches
    {column, type, string}
  end

  def parse_null_or_value(string, type) do
    case string do
      <<"null", rest::binary>> -> {nil, rest}
      _ -> parse_value(string, type)
    end
  end

  def parse_value(string, type) do
    case type do
      "integer" -> parse_integer(string)
      "text" -> parse_string(string)
      "character varying" -> parse_string(string)
      "timestamp without time zone" -> parse_datetime(string)
      "boolean" -> parse_boolean(string)
      "jsonb" -> parse_json(string)
    end
  end

  def parse_datetime(string) do
    use Timex
    {date, string} = parse_string(string)
    {date, string}
  end

  def parse_json(string) do
    {json, rest} = parse_string(string)
    {Poison.decode!(json), rest}
  end

  def parse_boolean(<<"true"::utf8, rest::binary>>), do: {true, rest}
  def parse_boolean(<<"false"::utf8, rest::binary>>), do: {false, rest}

  def parse_integer(string) do
    {[number | _], rest} = parse_regex(string, ~r/(\d+)/)
    {String.to_integer(number), rest}
  end

  def parse_string(string) do
    case string do
      <<"''"::utf8, rest::binary>> -> {"", rest}
      <<"'"::utf8, rest::binary>> -> _parse_string(rest, "")
    end
  end

  def _parse_string(<<"''"::utf8, rest::binary>>, acc), do: _parse_string(rest, acc <> "'")
  def _parse_string(<<"'"::utf8, rest::binary>>, acc), do: {acc, rest}
  def _parse_string(<<c::utf8, rest::binary>>, acc), do: _parse_string(rest, acc <> <<c::utf8>>)

end
