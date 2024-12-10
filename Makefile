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
.PHONY: format

lint: ## Run linting tools (cargo-clippy)
	@rustup component add clippy 2> /dev/null
	cargo clippy --workspace --all-targets --all-features --no-deps --allow-dirty -- -D warnings
.PHONY: format

format: ## Run autofix mode for formatting and lint
	@rustup component add clippy 2> /dev/null
	@rustup component add rustfmt 2> /dev/null
	cargo fmt --all
	cargo clippy --workspace --all-targets --all-features --no-deps --fix --allow-dirty --allow-staged -- -D warnings

# Tests

unit-test:
	cargo test
.PHONY: test

install-py-dev:
	python -m venv python/.venv
	. python/.venv/bin/activate
	pip install -r python/requirements-dev.txt
.PHONY: install-py-dev

integration-test:
	cargo build
	python -m venv python/.venv
	. python/.venv/bin/activate
	python -m pytest python/integration_tests -s -vv
.PHONY: integration-test

# Help

help: ## this help
	@ awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m\t%s\n", $$1, $$2 }' $(MAKEFILE_LIST) | column -s$$'\t' -t
.PHONY: help
