# Development
setup: setup-git install-py-dev ## run setup tasks to create and configure a development environment
.PHONY: setup

setup-git: .git/hooks/pre-commit ## Setup git-hooks
.PHONY: setup-git

.git/hooks/pre-commit: ## Symlink the precommit script
	@cd .git/hooks && ln -sf ../../scripts/git-precommit-hook pre-commit

# Builds

build: ## Build all features without debug symbols
	cargo build --all-features
.PHONY: build

release: ## Build a release profile with all features
	cargo build --all-features --release
.PHONY: release

# Linting and style

style: ## Run style checking tools (cargo-fmt)
	@rustup component add rustfmt 2> /dev/null
	cargo fmt --all --check
.PHONY: style

lint: ## Run linting tools (cargo-clippy)
	@rustup component add clippy 2> /dev/null
	cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings
.PHONY: lint

format: ## Run autofix mode for formatting and lint
	@rustup component add clippy 2> /dev/null
	@rustup component add rustfmt 2> /dev/null
	cargo fmt --all
	cargo clippy --workspace --all-targets --all-features --no-deps --fix --allow-dirty --allow-staged -- -D warnings
.PHONY: format

# Tests

unit-test: ## Run unit tests
	cargo test
.PHONY: unit-test

install-py-dev: ## Install python dependencies
	python -m venv python/.venv
	. python/.venv/bin/activate
	pip install -r python/requirements-dev.txt
.PHONY: install-py-dev

reset-kafka: install-py-dev ## Reset kafka
	devservices down
	-docker volume rm kafka_kafka-data
	devservices up
.PHONY: reset-kafka

test-rebalance: build reset-kafka ## Run the rebalance integration test
	python -m pytest python/integration_tests/test_consumer_rebalancing.py -s
	rm -r python/integration_tests/.tests_output/test_consumer_rebalancing
.PHONY: test-rebalance

test-worker-processing: build reset-kafka ## Run the worker processing integration test
	python -m pytest python/integration_tests/test_task_worker_processing.py -s
	rm -r python/integration_tests/.tests_output/test_task_worker_processing
.PHONY: test-worker-processing

test-upkeep-retry: build reset-kafka ## Run the upkeep retry integration test
	python -m pytest python/integration_tests/test_upkeep_retry.py -s
	rm -r python/integration_tests/.tests_output/test_upkeep_retry
.PHONY: test-upkeep-retry

test-upkeep-dlq: build reset-kafka ## Run the upkeep dlq integration test
	python -m pytest python/integration_tests/test_upkeep_dlq.py -s
	rm -r python/integration_tests/.tests_output/test_upkeep_dlq
.PHONY: test-upkeep-dlq

integration-test: test-rebalance test-worker-processing test-upkeep-retry ## Run all integration tests
.PHONY: integration-test

# Help

help: ## this help
	@ awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m\t%s\n", $$1, $$2 }' $(MAKEFILE_LIST) | column -s$$'\t' -t
.PHONY: help
