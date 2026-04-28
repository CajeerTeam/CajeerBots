.PHONY: install doctor run-all ci release
install:
	./scripts/install.sh
doctor:
	./scripts/doctor.sh --offline
run-all:
	./scripts/run.sh all
ci:
	python -m compileall -q core bots modules plugins
	EVENT_SIGNING_SECRET=ci-secret python -m core doctor --offline
release:
	./scripts/release.sh
