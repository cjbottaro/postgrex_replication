defmodule PgCdc.Mixfile do
  use Mix.Project

  def project do
    [app: :pg_cdc,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {:postgrex, "~> 0.12"},
      {:connection, ">= 1.0.0"},
      {:poison, "~> 3.0"},
      {:timex, "~> 3.0"},
      {:gnat, git: "https://github.com/cjbottaro/gnat"}
    ]
  end
end
