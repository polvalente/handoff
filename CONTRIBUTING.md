# Contributing to Handoff

Thank you for your interest in contributing to Handoff! This document provides guidelines and instructions for contributing to the project.

## Code of Conduct

Please note that this project adheres to a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Getting Started

### Prerequisites

- Elixir 1.18 or later
- Erlang/OTP 25 or later
- Git

### Setup

1. Fork the repository
2. Clone your fork:

   ```bash
   git clone https://github.com/YOUR_USERNAME/handoff.git
   cd handoff
   ```

3. Install dependencies:

   ```bash
   mix deps.get
   ```

4. Run the tests:

   ```bash
   mix test
   ```

### Development Workflow

1. Create a feature branch:

   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes
3. Ensure your code passes all checks:

   ```bash
   mix quality  # Runs formatter, Credo
   mix test     # Runs tests
   ```

4. Commit your changes with a descriptive message
5. Push your branch to your fork
6. Submit a pull request to the main repository

## Project Structure

```
handoff/
├── lib/                  # Source code
│   ├── handoff.ex        # Main module and public API
│   └── handoff/          # Implementation modules
│       ├── dag.ex        # DAG construction and validation
│       ├── function.ex   # Function struct definition
│       ├── distributed_executor.ex   # Execution engine
│       └── ...
├── test/                 # Test files
├── config/               # Configuration
```

## Coding Guidelines

### Style Guide

- Follow the [Elixir Style Guide](https://github.com/christopheradams/elixir_style_guide)
- Use `mix format` to format your code
- Address all Credo warnings with `mix credo --strict`

### Documentation

- Document all public functions with `@doc` and `@moduledoc`
- Include examples in documentation when applicable
- Document type specifications with `@type` and `@spec`

### Testing

- Write tests for all new functionality
- Aim for high test coverage (check with `mix test_coverage`)
- Use property-based testing with StreamData for appropriate cases
- Keep tests organized in a structure mirroring the lib/ directory

## Pull Request Process

1. Ensure your PR addresses a specific issue or feature
2. Update documentation as needed for changes
3. Include tests for new functionality
4. Update the [CHANGELOG.md](CHANGELOG.md) with your changes
5. Make sure CI passes on your PR
6. Wait for review by maintainers

### PR Title Format

Use the following format for PR titles:

- `feat: Add new feature`
- `fix: Fix bug in module`
- `docs: Update documentation`
- `test: Add tests for feature`
- `refactor: Refactor code`
- `chore: Update dependencies`

## Versioning

Handoff follows [Semantic Versioning](https://semver.org/):

- MAJOR version for incompatible API changes
- MINOR version for backward-compatible functionality
- PATCH version for backward-compatible bug fixes

## Release Process

1. Update version in `mix.exs`
2. Update `CHANGELOG.md`
3. Create a git tag with the version:

   ```bash
   git tag -a v0.2.0 -m "Release version 0.2.0"
   git push origin v0.2.0
   ```

4. The CI will automatically publish to Hex

## Testing on Multiple Nodes

To test distributed functionality locally:

```bash
# Terminal 1: Start the first node
iex --name node1@127.0.0.1 -S mix

# Terminal 2: Start the second node
iex --name node2@127.0.0.1 -S mix

# Terminal 3: Start the third node
iex --name node3@127.0.0.1 -S mix

# In Terminal 1, connect to other nodes
Node.connect(:"node2@127.0.0.1")
Node.connect(:"node3@127.0.0.1")
```

## Additional Resources

- [Elixir Documentation](https://elixir-lang.org/docs.html)
- [Hex Docs for Handoff](https://hexdocs.pm/handoff) (when published)
- [Project Roadmap](ROADMAP.md)

## Getting Help

If you have questions or need help, please:

1. Check existing issues and discussions
2. Open a new discussion for questions
3. Open an issue for bugs or feature requests

## License

By contributing to Handoff, you agree that your contributions will be licensed under the project's [MIT License](LICENSE).
