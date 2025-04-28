#!/bin/bash

# Enable distributed mode for Elixir
export ERL_AFLAGS="-sname primary@127.0.0.1"

# Run the tests
echo "Running tests with distributed nodes..."
mix test $@