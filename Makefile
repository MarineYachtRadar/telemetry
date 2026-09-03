# Telemetry collector build system
#
# Usage:
#   make          - Build the release binary
#   make debug    - Build the debug binary
#   make test     - Run the tests
#   make check    - Formatting and lints, the way CI would run them
#   make run      - Run a collector against a local database
#   make clean    - Clean build artifacts
#
# Deploying is not here: which machine this collector runs on is not part of
# the collector. See the comment at the bottom of this file.

.PHONY: all release debug test check run clean

all: release

release:
	cargo build --release

debug:
	cargo build

test:
	cargo test

check:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings

# A collector on loopback with its own database, so a local run cannot touch
# anything deployed.
run:
	cargo run -- --listen 127.0.0.1:8099 --database telemetry.db

clean:
	cargo clean

# Optional per-developer extensions: targets that should never be committed,
# such as how to reach the machine this collector is deployed on. The leading
# dash makes the include silent when the file is absent, so a fresh clone
# behaves identically to having no file. `Makefile.local` is gitignored.
-include Makefile.local
