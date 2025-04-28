# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build/Test Commands

- Mix compile: `mix compile`
- Format code: `mix format`
- Run all tests: `mix test`
- Run a single test: `mix test test/path/to/test_file.exs:line_number`
- Run tests with specific tags: `mix test --only tag_name`
- Generate documentation: `mix docs`

## Code Style Guidelines

- Formatting: Use `mix format` for consistent Elixir formatting
- Module names: PascalCase (e.g., `DistributedExecutor.Function`)
- Function/variable names: snake_case (e.g., `process_data`, `has_cuda`)
- Documentation: Include @moduledoc and @doc for public functions with examples
- Error handling: Use {:ok, result} | {:error, reason} patterns
- Struct definitions: Define all default values, document fields with comments
- Imports: Group imports by source (Elixir core, 3rd party, internal)
- Callback interfaces: Use @callback with proper typespecs
- Testing: Use doctest where applicable, organize with describe/test blocks