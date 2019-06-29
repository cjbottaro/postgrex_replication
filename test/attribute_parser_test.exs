defmodule AttributeParserTest do
  use ExUnit.Case

  alias PgCdc.AttributeParser

  def assert_value(string, value) do
    {{_column, _type, parsed_value}, _rest} = AttributeParser.parse(string)
    assert value == parsed_value
  end

  test "simple strings" do
    assert_value "name[text]:'chris' ", "chris"
  end

  test "escaped quotes in strings" do
    assert_value "name[text]:'chris''s' ", "chris's"
  end

  test "empty strings" do
    assert_value "name[text]:'' ", ""
  end

  test "empty strings at end" do
    assert_value "name[text]:''", ""
  end

end
