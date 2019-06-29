defmodule PgCdc.Change do
  defstruct [:table, :type, :changes, :attributes]

  import PgCdc.AttributeParser, only: [parse_regex: 2]

  def parse(string) do
    {table, string} = parse_table(string)
    {type, string} = parse_type(string)

    string = String.replace_leading(string, "old-key: ", "")
    {old_attributes, string} = parse_attributes(string)

    string = String.replace_leading(string, "new-tuple: ", "")
    {new_attributes, _} = parse_attributes(string)

    changes = Enum.reduce(old_attributes, %{}, fn {column, old_value}, acc ->
      new_value = new_attributes[column]
      if old_value != new_value do
        Map.put(acc, column, {old_value, new_value})
      else
        acc
      end
    end)

    %__MODULE__{ table: table, type: type, changes: changes, attributes: new_attributes }
  end

  def parse_table(string) do
    {[table | _], rest} = parse_regex(string, ~r/table (\w+\.\w+): /)
    {table, rest}
  end

  def parse_type(string) do
    {[type | _], rest} = parse_regex(string, ~r/(\w+): /)
    {type, rest}
  end

  def parse_attributes(string, acc \\ %{}) do
    cond do
      String.length(string) == 0 -> {acc, string}
      String.starts_with?(string, "new-tuple") -> {acc, string}
      true ->
        {{column, _, value}, string} = PgCdc.AttributeParser.parse(string)
        parse_attributes(string, Map.put(acc, column, value))
    end
  end

end
