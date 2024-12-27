.PHONY: build
build:
	dune build

.PHONY: test
test:
	dune runtest

.PHONY: setup
setup:
	nix develop
