.PHONY: build run-container run-interactive run-notebooks \
        run-standardize-pipeline run-normalize-pipeline \
        run-standardize-normalize-pipeline clean

# Define constants
mkfile_path := $(abspath $(firstword $(MAKEFILE_LIST)))
current_dir := $(notdir $(patsubst %/,%,$(dir $(mkfile_path))))
current_abs_path := $(subst Makefile,,$(mkfile_path))

# Project constants
project_image_name := 2024-winter-clinic-climate-cabinet
project_container_name := 2024-winter-clinic-climate-cabinet-container
project_dir := $(current_abs_path)

# Build Docker image
build:
	docker build -t $(project_image_name) -f Dockerfile $(current_abs_path)

# Helper for running containers
run-container:
	docker run --rm -v $(current_abs_path):/project -t $(project_image_name) $(cmd)

run-interactive: build
	docker run -it --rm -v $(current_abs_path):/project -t $(project_image_name) /bin/bash

run-notebooks: build
	docker run --rm -v $(current_abs_path):/project -p 8888:8888 -t $(project_image_name) \
	jupyter lab --port=8888 --ip='*' --NotebookApp.token='' --NotebookApp.password='' \
	--no-browser --allow-root

run-standardize-pipeline: build
	$(MAKE) run-container cmd="python scripts/standardize_pipeline.py"

run-normalize-pipeline: build
	$(MAKE) run-container cmd="python scripts/normalize_pipeline.py"

run-standardize-normalize-pipeline: build
	$(MAKE) run-standardize-pipeline
	$(MAKE) run-normalize-pipeline

# Clean up Docker resources
clean:
	docker system prune -f

