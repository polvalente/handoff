defmodule Handoff.MixProject do
  use Mix.Project

  @source_url "https://github.com/polvalente/handoff"
  @version "0.1.0"

  def project do
    [
      app: :handoff,
      name: "Handoff",
      description: "A distributed computing framework for Elixir",
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      docs: docs(),
      preferred_cli_env: [
        docs: :docs,
        "hex.publish": :docs
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Handoff.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # Development and testing dependencies
      {:ex_doc, "~> 0.29.1", only: :docs, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp aliases do
    [
      quality: ["format", "credo --strict"]
    ]
  end

  defp package do
    [
      maintainers: ["Paulo Valente"],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "getting_started",
      source_url_pattern: "#{@source_url}/blob/v#{@version}/handoff/%{path}#L%{line}",
      extras: Path.wildcard("guides/**/*.md") ++ Path.wildcard("livebooks/**/*.livemd"),
      groups_for_extras: [
        Guides: ~r"^guides/",
        Livebooks: ~r"^livebooks/"
      ]
    ]
  end
end
