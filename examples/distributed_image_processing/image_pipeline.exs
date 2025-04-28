#!/usr/bin/env elixir
# Distributed image processing example using Handout

# Ensure we can access the Handout library
Code.require_file("../../lib/handout.ex")
Code.require_file("image_transformations.ex")

defmodule DistributedImageProcessing do
  @moduledoc """
  Example of distributed image processing using Handout.
  """

  alias Handout.Function

  @doc """
  Run the distributed image processing pipeline.
  """
  def run do
    # Start Handout
    Handout.start()

    # Register the local node with capabilities
    IO.puts("Registering local node capabilities...")
    Handout.register_local_node(%{
      cpu: 8,         # 8 CPU cores
      memory: 16000,  # 16GB RAM
      gpu: 1          # 1 GPU
    })

    # Discover other nodes in the cluster
    {:ok, discovered_nodes} = Handout.discover_nodes()
    IO.puts("Discovered nodes: #{inspect Map.keys(discovered_nodes)}")

    # Simulate image paths (would be real paths in a production app)
    image_paths = Enum.map(1..5, fn i -> "/path/to/image_#{i}.jpg" end)
    IO.puts("Processing #{length(image_paths)} images...")

    # Create a DAG for image processing
    dag = build_pipeline(image_paths)

    # Validate the DAG
    case Handout.DAG.validate(dag) do
      {:ok, valid_dag} ->
        IO.puts("\nExecuting distributed image processing pipeline...\n")

        # Execute the DAG across all available nodes
        case Handout.execute_distributed(valid_dag,
          allocation_strategy: :cost_optimized,
          max_retries: 2
        ) do
          {:ok, results} ->
            # Show the saved collage path
            collage_result = results[:save_collage]
            IO.puts("\nPipeline completed successfully!")
            IO.puts("Collage saved to: #{collage_result.saved_path}")

            # Show quality metrics
            metrics = results[:quality_report]
            IO.puts("\nQuality metrics:")
            Enum.each(metrics, fn {image_id, {_img, report}} ->
              IO.puts("  #{image_id}: " <>
                "Sharpness: #{Float.round(report.sharpness, 1)}, " <>
                "Noise: #{Float.round(report.noise_level, 1)}")
            end)

            # Show node allocation statistics
            IO.puts("\nNode allocation:")
            nodes_used = Enum.map(valid_dag.functions, fn {id, func} ->
              {id, func.node}
            end) |> Enum.into(%{})

            node_counts = Enum.reduce(nodes_used, %{}, fn {_id, node}, acc ->
              Map.update(acc, node, 1, &(&1 + 1))
            end)

            Enum.each(node_counts, fn {node, count} ->
              IO.puts("  #{node}: #{count} functions")
            end)

            {:ok, results}

          {:error, reason} ->
            IO.puts("Error executing pipeline: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, reason} ->
        IO.puts("Invalid DAG: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Build the image processing pipeline DAG.
  """
  def build_pipeline(image_paths) do
    dag = Handout.new()

    # 1. Load images function
    load_images_fn = %Function{
      id: :load_images,
      args: [],
      code: fn ->
        IO.puts("Loading #{length(image_paths)} images...")
        image_paths |> Enum.map(&{&1, ImageTransformations.load_image(&1)}) |> Enum.into(%{})
      end,
      cost: %{cpu: 1, memory: 500}  # Light CPU, moderate memory
    }

    # 2. Resize images function
    resize_fn = %Function{
      id: :resize_images,
      args: [:load_images],
      code: fn %{load_images: images} ->
        IO.puts("Resizing images...")
        images |> Enum.map(fn {path, img} ->
          {path, ImageTransformations.resize_image(img, 800, 600)}
        end) |> Enum.into(%{})
      end,
      cost: %{cpu: 2, memory: 1000}  # Moderate CPU
    }

    # 3a. Apply grayscale effect
    grayscale_fn = %Function{
      id: :grayscale_effect,
      args: [:resize_images],
      code: fn %{resize_images: images} ->
        IO.puts("Applying grayscale effect...")
        images |> Enum.map(fn {path, img} ->
          {path, ImageTransformations.grayscale(img)}
        end) |> Enum.into(%{})
      end,
      cost: %{cpu: 2, memory: 800}  # Moderate CPU
    }

    # 3b. Apply sepia effect (parallel branch)
    sepia_fn = %Function{
      id: :sepia_effect,
      args: [:resize_images],
      code: fn %{resize_images: images} ->
        IO.puts("Applying sepia effect...")
        images |> Enum.map(fn {path, img} ->
          {path, ImageTransformations.sepia(img)}
        end) |> Enum.into(%{})
      end,
      cost: %{cpu: 2, memory: 800}  # Moderate CPU
    }

    # 3c. Apply blur effect (parallel branch)
    blur_fn = %Function{
      id: :blur_effect,
      args: [:resize_images],
      code: fn %{resize_images: images} ->
        IO.puts("Applying blur effect...")
        images |> Enum.map(fn {path, img} ->
          {path, ImageTransformations.blur_image(img)}
        end) |> Enum.into(%{})
      end,
      cost: %{cpu: 3, memory: 1200}  # Higher CPU, more memory
    }

    # 3d. Apply edge detection (GPU intensive)
    edge_fn = %Function{
      id: :edge_detection,
      args: [:resize_images],
      code: fn %{resize_images: images} ->
        IO.puts("Applying edge detection...")
        images |> Enum.map(fn {path, img} ->
          {path, ImageTransformations.edge_detection(img)}
        end) |> Enum.into(%{})
      end,
      cost: %{cpu: 2, memory: 1500, gpu: 1}  # Needs GPU
    }

    # 4. Create thumbnails
    thumbnail_fn = %Function{
      id: :create_thumbnails,
      args: [:resize_images],
      code: fn %{resize_images: images} ->
        IO.puts("Creating thumbnails...")
        images |> Enum.map(fn {path, img} ->
          {path, ImageTransformations.create_thumbnail(img)}
        end) |> Enum.into(%{})
      end,
      cost: %{cpu: 1, memory: 500}  # Light work
    }

    # 5. Quality assessment on different effects
    quality_assessment_fn = %Function{
      id: :quality_report,
      args: [:grayscale_effect, :sepia_effect, :blur_effect, :edge_detection],
      code: fn results ->
        IO.puts("Assessing image quality across effects...")

        # Combine all effect results
        all_effects = %{
          grayscale: results.grayscale_effect,
          sepia: results.sepia_effect,
          blur: results.blur_effect,
          edge: results.edge_detection
        }

        # Assess quality for each effect and image
        all_effects |> Enum.flat_map(fn {effect, images} ->
          images |> Enum.map(fn {path, img} ->
            {"#{effect}_#{Path.basename(path)}", ImageTransformations.assess_quality(img)}
          end)
        end) |> Enum.into(%{})
      end,
      cost: %{cpu: 4, memory: 2000}  # CPU intensive analysis
    }

    # 6. Create collage from effects
    collage_fn = %Function{
      id: :create_collage,
      args: [:grayscale_effect, :sepia_effect, :blur_effect, :edge_detection],
      code: fn results ->
        IO.puts("Creating collage from all effects...")

        # Extract all images from different effects
        all_images =
          results.grayscale_effect |> Map.values() |>
          Enum.concat(Map.values(results.sepia_effect)) |>
          Enum.concat(Map.values(results.blur_effect)) |>
          Enum.concat(Map.values(results.edge_detection))

        # Create the collage
        ImageTransformations.create_collage(all_images, width: 1600, height: 1200)
      end,
      cost: %{cpu: 4, memory: 4000}  # Heavy CPU and memory
    }

    # 7. Save thumbnails
    save_thumbnails_fn = %Function{
      id: :save_thumbnails,
      args: [:create_thumbnails],
      code: fn %{create_thumbnails: thumbnails} ->
        IO.puts("Saving thumbnails...")
        thumbnails |> Enum.map(fn {path, img} ->
          output_path = "output/thumb_#{Path.basename(path)}"
          {path, ImageTransformations.save_image(img, output_path)}
        end) |> Enum.into(%{})
      end,
      cost: %{cpu: 1, memory: 500}  # I/O bound
    }

    # 8. Save collage
    save_collage_fn = %Function{
      id: :save_collage,
      args: [:create_collage],
      code: fn %{create_collage: collage} ->
        IO.puts("Saving final collage...")
        output_path = "output/collage_#{DateTime.utc_now() |> DateTime.to_unix()}.jpg"
        ImageTransformations.save_image(collage, output_path)
      end,
      cost: %{cpu: 1, memory: 500}  # I/O bound
    }

    # Build the DAG
    dag
    |> Handout.DAG.add_function(load_images_fn)
    |> Handout.DAG.add_function(resize_fn)
    |> Handout.DAG.add_function(grayscale_fn)
    |> Handout.DAG.add_function(sepia_fn)
    |> Handout.DAG.add_function(blur_fn)
    |> Handout.DAG.add_function(edge_fn)
    |> Handout.DAG.add_function(thumbnail_fn)
    |> Handout.DAG.add_function(quality_assessment_fn)
    |> Handout.DAG.add_function(collage_fn)
    |> Handout.DAG.add_function(save_thumbnails_fn)
    |> Handout.DAG.add_function(save_collage_fn)
  end
end

# If script is run directly, execute the pipeline
if Code.ensure_loaded?(System) && System.argv() != [] do
  DistributedImageProcessing.run()
end
