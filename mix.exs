defmodule Handout.MixProject do
  use Mix.Project

  def project do
    [
      app: :handout,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # Runtime dependencies
      {:telemetry, "~> 1.2"},
      {:jason, "~> 1.4"},

      # Development and testing dependencies
      {:ex_doc, "~> 0.29.1", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp aliases do
    [
      quality: ["format", "credo --strict"]
    ]
  end
end
