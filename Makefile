REPOSITORY ?= git.marcet.info/javier/movistar-u7d
PLATFORMS  ?= linux/amd64,linux/arm64
TAG        ?= unstable

PY_FILES    = mu7d.py mu7d_cfg.py mu7d_lib.py mu7d_tvg.py mu7d_vod.py setup.py
CONF_FILES  = $(wildcard mu7d.conf)

.PHONY: help format lint test image image-slim devcontainer buildx freeze start stop restart up down logs clean

help:  ## Show this help
	@awk -F':.*## ' '/^[a-z-]+:.*## / { printf "%-14s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

format:  ## Format the code and fix what ruff can
	ruff format $(PY_FILES)
	ruff check --fix $(PY_FILES)

lint:  ## Check the code with ruff, bandit, pycodestyle & pylint
	ruff format --check $(PY_FILES)
	ruff check $(PY_FILES)
	bandit $(PY_FILES)
	pycodestyle $(PY_FILES)
	pylint $(PY_FILES)

test: lint  ## Lint plus compile & sample config checks
	python3 -m py_compile $(PY_FILES)
	$(if $(CONF_FILES),python3 -c "import tomli; tomli.loads(open('$(CONF_FILES)', encoding='utf8').read().lstrip('﻿'))")

image:  ## Build the full amd64 Docker image
	docker buildx build --build-arg BUILD_TYPE=full --platform linux/amd64 --load -t $(REPOSITORY):$(TAG) .

image-slim:  ## Build the slim amd64 Docker image
	docker buildx build --platform linux/amd64 --load -t $(REPOSITORY):$(TAG)-slim .

devcontainer:  ## Build the devcontainer image
	docker build -f .devcontainer/Dockerfile -t $(REPOSITORY):devcontainer .

buildx:  ## Build & push the multiarch images, as the CI does
	docker buildx build --build-arg BUILD_TYPE=full --platform $(PLATFORMS) --provenance false --push -t $(REPOSITORY):$(TAG) .
	docker buildx build --platform $(PLATFORMS) --provenance false --push -t $(REPOSITORY):$(TAG)-slim .

freeze:  ## Record the versions installed in this image
	uv pip freeze --system | grep -v '^uv==' > requirements-frozen.txt
	uv pip compile -U --no-annotate --no-header -q --python-platform windows requirements-win.txt -o requirements-frozen-win.txt

start: ## Start mu7d with init service
	/etc/init.d/mu7d start

stop: ## Stop m7d with init service
	/etc/init.d/mu7d stop

restart: ## Restart mu7d with init service
	/etc/init.d/mu7d restart

up:  ## Start mu7d with docker compose
	docker compose up -d

down:  ## Stop mu7d with docker compose
	docker compose down

logs:  ## Follow the proxy logs
#	docker compose logs -f --tail 100
	docker logs -f movistar_u7d

clean:  ## Remove the python & ruff caches
	rm -fr __pycache__ .ruff_cache
