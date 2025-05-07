defmodule Handoff.ResourceTracker do
  @moduledoc """
  Behavior for tracking and managing resources across nodes in the cluster.

  This module is responsible for:
  - Registering nodes with their capabilities
  - Checking resource availability on nodes
  - Allocating resources for functions
  - Releasing resources when functions complete
  """

  @doc """
  Register a node with its capabilities.

  ## Parameters
  - `node`: The node to register
  - `caps`: Map of capabilities/resources the node provides
  """
  @callback register(node :: node(), caps :: map()) :: :ok

  @doc """
  Check if the specified node has the required resources available.

  ## Parameters
  - `node`: The node to check
  - `req`: Map of resource requirements to check
  """
  @callback available?(node :: node(), req :: map()) :: boolean()

  @doc """
  Request resources from a node for a function execution.

  ## Parameters
  - `node`: The node to request resources from
  - `req`: Map of resource requirements
  """
  @callback request(node :: node(), req :: map()) :: :ok | {:error, :resources_unavailable}

  @doc """
  Release resources back to the pool after function execution.

  ## Parameters
  - `node`: The node to release resources on
  - `req`: Map of resources to release
  """
  @callback release(node :: node(), req :: map()) :: :ok
end
